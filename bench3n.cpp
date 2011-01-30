/*
 * Copyright (c) 2011
 *	Donald D. Anderson.  All rights reserved.
 *
 * Redistribution and use in source and binary forms are permitted.
 * This software is provided 'as is' and any express or
 * implied warranties, including, but not limited to, the implied
 * warranties of merchantability, fitness for a particular purpose, or
 * non-infringement, are disclaimed.
 */

#include "db.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <iostream>
#include <sstream>
#include "bench3n.h"

#define STORE_AS_STRING

using namespace std;

const int compute_chunk_size = 1000;

const char *testdir = NULL;

static DB_ENV *curdbenv;
static volatile int running = 1;

typedef int cyclelength_t;

enum TxnType {
    SYNC, NOSYNC, WRITENOSYNC, NONE
};

// These are arguments - input and output, past to each worker thread
struct bench_args {
    // these are input args, only read by the thread
    DB *benchdb;
    int minn;
    int maxn;
    int threads;
    int cachek;
    int logbufsizek;
    int logflags;
    int tricklepercent;
    int trickleinterval;
    int partition;
    TxnType txn;
    bool sortbylength;

    // these are output or 'return' args, only written by the thread
    cyclelength_t ret_cycles;
    long ret_nput;
    long ret_nget;
};

const char *digits[] =
{
    "zero", "eins", "deux",
    "tres", "quattro", "пять",
    "ستة", "सात", "捌", "ஒன்பது"
};

static void dump_digits()
{
    for (int i=0; i<10; i++)
    {
        //fprintf(stderr, "digit[%d] len=%d \"%s\"\n", i, (int)strlen(digits[i]), digits[i]);
    }
}

inline int nspace(unsigned char *p, int len)
{
    int count = 0;
    while (len-- > 0) {
        if (*p++ == ' ')
            count++;
    }
    return count;
}

int key_digit_length_compare(DB *db, const DBT *dbt1, const DBT *dbt2)
{
    unsigned char *p1 = (unsigned char *)dbt1->data;
    unsigned char *p2 = (unsigned char *)dbt2->data;
    int nspace1 = nspace(p1, dbt1->size);
    int nspace2 = nspace(p2, dbt2->size);

    if (nspace1 < nspace2)
        return -1;
    else if (nspace1 > nspace2)
        return 1;

    // quick compare - we know that all keys have length > 0
    if (*p1 < *p2)
        return -1;
    else if (*p1 > *p2)
        return 1;

    int len = dbt1->size;
    if (dbt2->size < len)
        len = dbt2->size;

    while (len-- > 0) {
        if (*p1 < *p2)
            return -1;
        else if (*p1 > *p2)
            return 1;
        p1++;
        p2++;
    }
    if (dbt1->size < dbt2->size)
        return -1;
    else if (dbt1->size > dbt2->size)
        return 1;

    return 0;
}

// Uses Fowler/Noll/Vo hash as taken from src/hash/hash_func.c in DB source

u_int32_t partitioner(DB *db, DBT *key)
{
    struct bench_args *args = (bench_args *)db->app_private;

    const u_int8_t *k = (u_int8_t *)key->data;
    const u_int8_t *e = k + key->size;
    u_int32_t h = 0;

    for (h = 0; k < e; ++k) {
        h *= 16777619;
        h ^= *k;
    }
    return (h % args->partition);
}


// Forces values to be stored in MSB format
class DatabaseLong
{
private:
    unsigned char bytes[8];
public:
    DatabaseLong(long l)
    {
        setLong(l);
    }

    long getLong()
    {
        long val = 0;
        for (int i=0; i<sizeof(bytes); i++)
        {
            val <<= 8;
            val |= bytes[i];
        }
        return val;
    }

    void setLong(long l)
    {
        for (int i=sizeof(bytes)-1; i>=0; i--)
        {
            bytes[i] = (l & 0xff);
            l >>= 8;
        }
    }
    
    unsigned char *getBytes()
    {
        return bytes;
    }

    size_t getSize()
    {
        return sizeof(long);
    }
};

class DatabaseDigits
{
private:
    string s;
public:
    DatabaseDigits(long l)
    {
        setLong(l);
    }

    DatabaseDigits(string &sparam)
    {
        s = sparam;
    }

    long getLong()
    {
        long val = 0;
        istringstream iss(s);
        do {
            string sub;
            iss >> sub;
            if (sub == "")
                break;
            val *= 10;
            for (int i=0; i<10; i++) {
                if (sub == digits[i]) {
                    val += i;
                    break;
                }
            }
        } while (iss);
        return val;
    }

    void setLong(long l)
    {
        if (l < 10) {
            if (l < 0) {
                // ERROR!!
                cerr << "bad call to setLong(" << l << ")\n";
                exit(1);
            }
            s = digits[(int)l];
        }
        else {
            s = "";
            while (l != 0) {
                if (s.length() != 0)
                    s.insert(0, " ");
                s.insert(0, digits[(int)(l % 10)]);
                l = l / 10;
            }
        }
    }
    
    unsigned char *getBytes()
    {
        return (unsigned char *)s.c_str();
    }

    size_t getSize()
    {
        return s.length();
    }

};

std::string db_path_name(const char *name)
{
    if (curdbenv == NULL) {
        return std::string(testdir) + "/" + name;
    }
    else {
        return name;
    }
}

static long next_value = -1;

mutex_handle get_value_mutex;

long get_next_value()
{
    long retval = 0;

    /*Critical section*/
    mutex_lock(&get_value_mutex);
    retval = next_value;
    next_value += compute_chunk_size;
    mutex_unlock(&get_value_mutex);
    /*End Critical section*/

    return retval;
}

cyclelength_t compute_cycles(bench_args *args, long n)
{
    if (n == 1) {
        return 1;
    }
    if (n <= 0) {
        fprintf(stderr, "bench3n: overflow/underflow\n");
        return -1;                  // TODO: throw exception
    }

#ifdef STORE_AS_STRING
    DatabaseDigits key(n);
    DatabaseDigits val(0);
    char stored[512];          // TODO: should avoid fixed size array.
#else
    DatabaseLong key(n);
    cyclelength_t val = 0;
#endif

    DBT keydbt;
    memset(&keydbt, 0, sizeof(DBT));
    keydbt.data = key.getBytes();
    keydbt.size = keydbt.ulen = key.getSize();
    keydbt.flags = DB_DBT_USERMEM;

    DBT valdbt;
    memset(&valdbt, 0, sizeof(DBT));
#ifdef STORE_AS_STRING
    valdbt.data = stored;
    valdbt.size = valdbt.ulen = sizeof(stored);
#else
    valdbt.data = &val;
    valdbt.size = valdbt.ulen = sizeof(cyclelength_t);
#endif
    valdbt.flags = DB_DBT_USERMEM;

    int ret;
    int flags = 0;
    flags |= DB_READ_UNCOMMITTED;
    args->ret_nget++;
    DEADLOCK_RETRY(args->benchdb->get(args->benchdb, NULL, &keydbt, &valdbt, flags), 5, "db", "get", ret);
    if (ret == 0)
    {
#ifdef STORE_AS_STRING
        stored[valdbt.size] = '\0';
        string ds(stored);
        DatabaseDigits d(ds);
        return d.getLong();
#else
        /*fprintf(stderr, "  found (%d) => %d\n", n, val);*/
        return val;
#endif
    }
    else if (ret != DB_NOTFOUND)
    {
        // Note: no exception, we can recover on get by doing more work.
        fprintf(stderr, "bench3n: warning: getting value %ld: %s\n", key.getLong(), db_strerror(ret));
    }

    long nextn = ((n % 2) == 0) ? (n/2) : (3*n + 1);
    cyclelength_t ncycles = compute_cycles(args, nextn);
    if (ncycles <= 0)
        return ncycles;             // error return
    ncycles++;

#ifdef STORE_AS_STRING
    DatabaseDigits d(ncycles);
    valdbt.data = d.getBytes();
    valdbt.size = valdbt.ulen = d.getSize();
#else
    val = ncycles;
#endif
    args->ret_nput++;
    DEADLOCK_RETRY(args->benchdb->put(args->benchdb, NULL, &keydbt, &valdbt, 0), 5, "db", "put", ret);
    if (ret == 0)
        return ncycles;
    else
    {
        fprintf(stderr, "bench3n: error: getting value %ld: %s\n", key.getLong(), db_strerror(ret));
        return -1;                  // TODO: throw exception
    }
}

void *bench_thread_main(void *thread_args)
{
    bench_args *args = (bench_args*)thread_args;
    cyclelength_t maxcycles = -1;
    for (long chunkstart = get_next_value(); chunkstart <= args->maxn; chunkstart = get_next_value()) {
        for (long n = chunkstart; n < chunkstart + compute_chunk_size; n++) {
            cyclelength_t ncycles = compute_cycles(args, n);
            if (ncycles > maxcycles)
                maxcycles = ncycles;
        }
    }
    //fprintf(stderr, "thread result: %d: %d\n", args->maxn, maxcycles);
    args->ret_cycles = maxcycles;
    return NULL;
}

void *trickle_thread_main(void *thread_args)
{
    bench_args *args = (bench_args*)thread_args;

    int pct = 5;
    int nsecs = args->trickleinterval;

    while (running) {
        if (args->tricklepercent < 0) {
            int npages = 0;
            curdbenv->memp_trickle(curdbenv, pct, &npages);
            if (npages > 0) {
                nsecs = 7;
                if (pct > 3)
                    pct--;
            }
            else if (pct < 5) {
                pct++;
            }
            else {
                nsecs = 20;
                pct = 5;
            }
        }
        else {
            curdbenv->memp_trickle(curdbenv, args->tricklepercent, NULL);
        }
        sleep(nsecs);
    }
}


void runbench(bench_args *args)
{
    thread_handle tids[args->threads];
    bench_args targs[args->threads];
    int nthreads = args->threads;
    int ret;
    cyclelength_t maxcycles = 0;
    long nput = 0;
    long nget = 0;

    next_value = args->minn;

    time_t startt, endt;
    int nseconds;

    time(&startt);
    for (int t=0; t<nthreads; t++) {
        targs[t] = *args;
        if ((ret = thread_start(bench_thread_main, &targs[t], &tids[t])) != 0) {
            fprintf(stderr, "thread_start error: %s\n", strerror(ret));
        }
        //fprintf(stderr, "Thread[%d] = %p started\n", t, tids[t]);
    }

    for (int t=0; t<nthreads; t++) {
        CHK(thread_join(tids[t]), "thread", "join");
        //fprintf(stderr, "Thread[%d] = %p joined\n", t, tids[t]);
    }
    time(&endt);
    nseconds = (int)(endt - startt);

    // collect the results
    for (int t=0; t<nthreads; t++) {
        if (targs[t].ret_cycles > maxcycles)
            maxcycles = targs[t].ret_cycles;
        nput += targs[t].ret_nput;
        nget += targs[t].ret_nget;
    }
    fprintf(stderr, "  N=%d\n  result=%d\n  time=%d\n", args->maxn, maxcycles, nseconds);
    fprintf(stderr, "  nputs=%ld (%.2f puts/second)\n  ngets=%ld (%.2f gets/second)\n  ops=%ld (%.2f ops/second)\n\n", nput, ((double)nput)/nseconds, nget, ((double)nget)/nseconds, (nput+nget), ((double)(nput+nget)/nseconds));
}

int openrunbench(bench_args *args)
{
    DB *db;
    int envflags;

    envflags = DB_CREATE | DB_INIT_MPOOL | DB_INIT_LOCK | DB_THREAD;
    CHK(db_env_create(&curdbenv, 0), "dbenv", "create");
    if (args->cachek != 0) {
        CHK(curdbenv->set_cachesize(curdbenv, 0, args->cachek * 1024, 0), "dbenv", "set_cachesize");
    }
    CHK(curdbenv->set_lk_detect(curdbenv, DB_LOCK_DEFAULT), "dbenv", "set_lk_detect");
    if (args->logbufsizek != 0) {
        CHK(curdbenv->set_lg_bsize(curdbenv, args->logbufsizek * 1024), "dbenv", "set_lg_bufsize");
    }
    if (args->logflags != 0) {
        CHK(curdbenv->log_set_config(curdbenv, args->logflags, 1), "dbenv", "log_set_config");
    }
    if (args->txn != NONE) {
        envflags |= DB_INIT_TXN;
        if (args->txn == NOSYNC) {
            CHK(curdbenv->set_flags(curdbenv, DB_TXN_NOSYNC, 1), "dbenv", "set_flags");
        }
        else if (args->txn == NOSYNC) {
            CHK(curdbenv->set_flags(curdbenv, DB_TXN_WRITE_NOSYNC, 1), "dbenv", "set_flags");
        }
    }
    CHK(curdbenv->open(curdbenv, testdir, envflags, 0), "dbenv->open", testdir);
    
    std::string pathnm = db_path_name("3ncycles.db");
    int flags = DB_CREATE;
    if (args->txn != NONE) {
        flags |= DB_AUTO_COMMIT;
    }
    flags |= DB_READ_UNCOMMITTED;

    CHK(db_create(&db, curdbenv, 0), "db", "create");
    db->app_private = args;
    if (args->sortbylength) {
        CHK(db->set_bt_compare(db, key_digit_length_compare), "db", "set compare");
    }
    if (args->partition > 0) {
        CHK(db->set_partition(db, args->partition, NULL, partitioner), "db", "set_partition");
    }
    CHK(db->open(db, NULL, pathnm.c_str(), NULL, DB_BTREE, flags, 0), "db->open", pathnm.c_str());

    args->benchdb = db;

    thread_handle trickle_thread;
    if (args->tricklepercent != 0) {
        CHK(thread_start(trickle_thread_main, args, &trickle_thread), "dbenv", "tricklethread");
    }

    runbench(args);
    CHK(args->benchdb->close(args->benchdb, 0), "db", "close");
    running = 0;
    if (args->tricklepercent != 0) {
        CHK(thread_join(trickle_thread), "trickle thread", "join");
    }
    CHK(curdbenv->close(curdbenv, 0), "dbenv", "close");

    return (0);
}

int main(int argc, char **argv) {
    bench_args args;

    memset(&args, 0, sizeof(args));
    args.threads = 1;
    args.minn = 1;
    args.cachek = 512;
    //args.tricklepercent = 20;
    //args.trickleinterval = 10;
    args.txn = NOSYNC;

    while (argc > 2) {
        const char *arg = argv[1];
        if (strcmp(arg, "-l") == 0) {
            args.minn = atoi(argv[2]);
            argv += 2;
            argc -= 2;
        }
        else if (strcmp(arg, "-n") == 0) {
            args.maxn = atoi(argv[2]);
            argv += 2;
            argc -= 2;
        }
        else if (strcmp(arg, "-c") == 0) {
            args.cachek = atoi(argv[2]);
            argv += 2;
            argc -= 2;
        }
        else if (strcmp(arg, "-t") == 0) {
            args.threads = atoi(argv[2]);
            argv += 2;
            argc -= 2;
        }
        else if (strcmp(arg, "-nosynctxn") == 0) {
            args.txn = NOSYNC;
            argv += 1;
            argc -= 1;
        }
        else if (strcmp(arg, "-writenosynctxn") == 0) {
            args.txn = WRITENOSYNC;
            argv += 1;
            argc -= 1;
        }
        else if (strcmp(arg, "-notxn") == 0) {
            args.txn = NONE;
            argv += 1;
            argc -= 1;
        }
        else if (strcmp(arg, "-synctxn") == 0) {
            args.txn = SYNC;
            argv += 1;
            argc -= 1;
        }
        else if (strcmp(arg, "-tricklepct") == 0) {
            args.tricklepercent = atoi(argv[2]);
            argv += 2;
            argc -= 2;
        }
        else if (strcmp(arg, "-logbufsize") == 0) {
            args.logbufsizek = atoi(argv[2]);
            argv += 2;
            argc -= 2;
        }
        else if (strcmp(arg, "-logdsync") == 0) {
            args.logflags |= DB_LOG_DSYNC;
            argv += 1;
            argc -= 1;
        }
        else if (strcmp(arg, "-logdirect") == 0) {
            args.logflags |= DB_LOG_DIRECT;
            argv += 1;
            argc -= 1;
        }
        else if (strcmp(arg, "-logzero") == 0) {
            args.logflags |= DB_LOG_ZERO;
            argv += 1;
            argc -= 1;
        }
        else if (strcmp(arg, "-trickleinterval") == 0) {
            args.trickleinterval = atoi(argv[2]);
            argv += 2;
            argc -= 2;
        }
        else if (strcmp(arg, "-sortbylength") == 0) {
            args.sortbylength = true;
            argv += 1;
            argc -= 1;
        }
        else if (strcmp(arg, "-partition") == 0) {
            args.partition = atoi(argv[2]);
            argv += 2;
            argc -= 2;
        }
        else {
            fprintf(stderr, "bench3n: bad arg=%s\n", arg);
            break;
        }
    }
    if (argc != 2 || args.maxn == 0) {
        fprintf(stderr, "Usage: bench3n "
          "-n maxN [ -l minN ] [ -t nthreads ] [ -c cachesize-in-kbytes ] dir\n");
        exit(1);
    }
    testdir = argv[1];

    dump_digits();
    mutex_init(&get_value_mutex);
    return openrunbench(&args);
}
