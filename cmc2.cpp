#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <fcntl.h>
#include <pthread.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <errno.h>
#include <math.h>
#include <string>
#include <sys/time.h>
#include <vector>
#include <sstream>

#define SCALE 1024
#define FV_LEN 512
#define SIZE (1*SCALE*SCALE*SCALE)
#define BLOCK_SIZE (SCALE)
#define BLOCK_NUM (SIZE / BLOCK_SIZE)
#define LOOP SCALE

// SLEEP TIME COMPUTING //

#include <iostream>
#include <chrono>
#include <time.h>
#include <thread>
#include <sstream>




void generate_fv (char* fv, size_t fv_len) {

    if (fv == NULL) {
        return;
    }
    
    int i = 0;

    for (i = 0; i < fv_len; i++) {
        fv[i] = rand() % 255;
    }

    return;
}

char* create_fv_batch (size_t fv_batch_num) {

    char* fv_batch_mem = NULL;

    fv_batch_mem = (char*)malloc(fv_batch_num * FV_LEN);

    int   i = 0;
    char* ptr = NULL;

    for (i = 0; i < fv_batch_num; i++) {
        ptr = fv_batch_mem + i * FV_LEN;
        generate_fv (ptr, FV_LEN);
    }

    return fv_batch_mem;
}

void free_fv_batch (char* fv_batch) {

    if (NULL != fv_batch) {
        free (fv_batch);
    }

    return;
}

char* create_target_fv_batch (size_t fv_batch_num) {

    return create_fv_batch (fv_batch_num);
}

void free_target_fv_batch(char* fv_batch) {

    free_fv_batch (fv_batch);
}

char* create_db_from_file (const char* file_path, size_t* db_size) {

    if (NULL == file_path) {
        return NULL;
    }

    // Get file size
    int ret = 0;
    struct stat statbuf;
    size_t file_len = 0;
    size_t fv_num = 0;
    ret = stat(file_path, &statbuf);
    if (0 != ret) {
        printf("ERROR: get file [%s] stat failed. (errno = %d)\n", file_path, errno);
    }
    file_len = statbuf.st_size;

    file_len = file_len / FV_LEN * FV_LEN;
    fv_num = file_len / FV_LEN;

    char* db_mem = NULL;
    if (file_len > 0) {
        db_mem = (char*) malloc(file_len);
        if (NULL == db_mem) {
            printf("ERROR: malloc db_mem (size = %ld) failed. (errno = %d)\n", file_len, errno);
            return NULL;
        }
    }

    FILE* fp;

    fp = fopen (file_path, "rb");
    if (NULL == fp) {
        printf("ERROR: fopen [%s] failed. (errno = %d)\n", file_path, errno);
        free (db_mem);
        return NULL;
    }

    ret = fread(db_mem, FV_LEN, fv_num, fp);
    if (0 == ret) {
        printf("ERROR: fread failed. (errno = %d)\n", errno);
        free (db_mem);
        fclose(fp);
        return NULL;
    }

    *db_size = ret*FV_LEN;

    fclose(fp);

    return db_mem;
}

char* create_db_from_memory (size_t db_size) {

    //size_t db_sz = db_size / FV_LEN * FV_LEN;
    if (db_size <= 0) {
        return NULL;
    }

    char* db_ptr = NULL;
    size_t fv_num = db_size / FV_LEN;
    db_ptr = create_fv_batch (fv_num);

    return db_ptr;
}

void store_db_to_file (char* db, size_t db_size, const char* file_path) {
    if (NULL == db || db_size <=0 || NULL == file_path) {
        printf("ERROR: store_db_to_file failed. Invalid Argument.\n");
        return;
    }

    int ret = 0;

    ret = access(file_path, 0);
    if (0 == ret) {
        printf("WARN: db file [%s] already exists. overwrite.\n", file_path);
        // return;
    }


    FILE* fp;

    fp = fopen (file_path, "wb");
    if (NULL == fp) {
        printf("ERROR: fopen [%s] failed. (errno = %d)\n", file_path, errno);
        return;
    }

    size_t fv_num = db_size / FV_LEN;
    ret = fwrite(db, FV_LEN, fv_num, fp);
    if (0 == ret) {
        printf("ERROR: fwrite failed. (errno = %d)\n", errno);
        fclose(fp);
        return;
    }

    fclose(fp);
}

void free_db (char* db) {

    if (NULL != db) {
        free (db);
    }

    return;
}

float get_mold (char* fv, size_t fv_size) {
    float sum = 0.0;
    for (int i = 0; i < fv_size; i++) {
        sum += fv[i] * fv[i];
    }
    return sqrt (sum);
}

float compare_fv (char* lhs, size_t lhs_size, char* rhs, size_t rhs_size) {

    if (lhs_size != rhs_size) {
        printf("ERROR: ERROR: compare_fv the size is different. %ld, %ld\n", lhs_size, rhs_size);
        return -1.0;
    }

    float tmp = 0.0;
    for (int i = 0; i < lhs_size; i++) {
        tmp += lhs[i] * rhs[i];
    }

    float similarity = tmp / (get_mold(lhs, lhs_size) * get_mold(rhs, rhs_size));

    return similarity;
}

float search_fv_in_db (char* fv, char* db, size_t db_size, int k=1) {

    if (NULL == fv || NULL == db || db_size <= 0) {
        printf("ERROR: search_fv_in_db Invalid Argument.\n");
        return -1.0;
    }

    int ret = 0;

    int i = 0;
    size_t fv_num = db_size / FV_LEN;
    char* fv_in_db = db;
    float comp_res = 0.0;
    float max_comp_res = -10000.0;

    for (i = 0; i < fv_num; i++) {
        comp_res = compare_fv (fv, FV_LEN, fv_in_db, FV_LEN);
        if (comp_res > max_comp_res) {
            max_comp_res = comp_res;
        }
        fv_in_db += FV_LEN;

        // if (i % 100 == 0) {
        //     printf("dbg: i = %d, similarity = %.2f, max_comp_res = %.2f\n", i, comp_res, max_comp_res);
        // }

    }

    return max_comp_res;

}

void search_fv_batch (char* fv_batch, size_t batch_size, char* db, size_t db_size, int k=1) {

    int ret = 0;
    int i = 0;
    char* fv_ptr = fv_batch;
    float res = 0.0;

    for (i = 0; i < batch_size; i++) {
        // printf("dbg: target fv %d/%ld\n", i, batch_size);
        res = search_fv_in_db (fv_ptr, db, db_size, k);
        printf("   --> batch %d/%ld result: max_comp_res = %.2f\n", i+1, batch_size, res);
    }

    return;
}

void print_helper () {

    const char* msg = \
    "\n HELPER MESSAGE:\n\
        -b : target feature vector batch size, example: 10, 32 etc.\n\
        -d : database size in byte, example: 1000000000 (1GB).\n\
        -s : store database into file path, example: ./db.data.\n\
        -f : restore database from file path, example: ./db.data.\n\
        -l : loop times, example: 3.\n\
        -t : start time, example: 2022-09-28-20:12:53. [the '-' in the middle is a MUST!]\n\
    ";

    printf ("%s\n\n", msg);
}

void wait_to_start (struct tm tt2)
{

    std::chrono::time_point<std::chrono::system_clock> tp1 = std::chrono::system_clock::now();
    //std::cout << "timepoint1: " << (tp1.time_since_epoch()).count() << std::endl;

    time_t ttt;
    ttt = mktime(&tt2);

    if (ttt < 0) {
       printf("ret ttt = %ld, errno = %d\n", ttt, errno);
       printf("tt2: %d %d %d %d %d %d %d\n", tt2.tm_year, tt2.tm_mon, tt2.tm_mday, tt2.tm_hour, tt2.tm_min, tt2.tm_sec, tt2.tm_isdst);
    }

    std::chrono::time_point<std::chrono::system_clock> tp2 = std::chrono::system_clock::from_time_t(ttt);
    //std::cout << "timepoint2: " << (tp2.time_since_epoch()).count() << std::endl;

    int wait_milli = std::chrono::duration_cast<std::chrono::milliseconds>(tp2-tp1).count();
    std::cout << "sleep for: " << wait_milli/1000/60 << " min " << wait_milli/1000 << " sec " << wait_milli%1000 << " millisec" << std::endl;

    std::this_thread::sleep_for(std::chrono::milliseconds(wait_milli));

    std::cout << "wake up, start to work." << std::endl;

    return;
}

struct ThreadArg
{
    int loop_num;
    char* target_fv;
    size_t target_fv_size;
    char* db;
    size_t db_size;
};

void thread_func(int index, struct ThreadArg* arg) {
    struct timeval t1, t2;
    double time_used = 0.0;

    gettimeofday (&t1, NULL);
    for (int i = 0; i < arg->loop_num; i++) {
       printf(" --> LOOP: %d/%d starts.\n", i+1, arg->loop_num);
       search_fv_batch (arg->target_fv, arg->target_fv_size, arg->db, arg->db_size);
    }
    gettimeofday (&t2, NULL);

    time_used = (t2.tv_sec - t1.tv_sec) + (double) (t2.tv_usec - t1.tv_usec)/1000000.0;

    size_t matching_amount = arg->loop_num * arg->target_fv_size * (arg->db_size/FV_LEN);
    float throughput = matching_amount / time_used;

    std::stringstream msg;
    msg << "\n\n["  << index << "] RESULT\n";
    msg << "["  << index << "] ==> matching_amount = loop_num * fv_batch_size * db_size / FV_LEN = " << arg->loop_num << "*" << arg->target_fv_size << "*" << arg->db_size << "*" << FV_LEN << "=" << matching_amount << "\n";
    msg << "["  << index << "] ==> time_used = " << time_used << " seconds\n";
    msg << "["  << index << "] ==> throughput = " << throughput << " times/sec\n";


    std::cout << msg.str() <<std::endl;

    // printf("\n\nRESULT:\n");
    // printf(" ==> overall_matching_amount = thread_num * loop_num * fv_batch_size * db_size / FV_LEN = %d * %d * %d * %ld * %d = %ld\n", thread_num, loop_num, target_fv_size, db_size, FV_LEN, overall_matching_amount);
    // printf(" ==> time_used = %.2f sec\n", time_used);
    // printf(" ==> overall_throughput = %.2f times/sec\n", overall_throughput);
}


int main (int argc, char** argv) {

    int i = 0;
    size_t db_size = 100*1024*1024; // 100 MByte
    size_t overall_matching_amount = 0;
    int target_fv_size = 1;
    int loop_num = 5;
    std::string to_db_path;
    std::string from_db_path;
    float overall_throughput = 0.0;
    //struct timeval = start_time {0, 0};

    struct tm start_time;
    start_time.tm_year = 0;
    int thread_num = 1;


    int opt;
    const char *optstring = "d:b:s:l:f:t:x:h?";

    while ((opt = getopt(argc, argv, optstring)) != -1) {
        switch (opt) {
            case 'b':
                // printf("ARGUMENT: b [batch size] = %s\n", optarg);
                if (NULL != optarg) {
                    target_fv_size = atoi(optarg);
                }
                break;
            case 'd':
                // printf("ARGUMENT: d [db size in Byte] = %s\n", optarg);
                if (NULL != optarg) {
                    db_size = strtoul(optarg, NULL, 10);
                }
                break;
            case 'l':
                // printf("ARGUMENT: l [loop num] = %s\n", optarg);
                if (NULL != optarg) {
                    loop_num = atoi(optarg);
                }
                break;
            case 's':
                // printf("ARGUMENT: s [store db to file] = %s\n", optarg);
                if (NULL != optarg) {
                    to_db_path = optarg;
                }                
                break;
            case 'f':
                // printf("ARGUMENT: f [restore db from file] = %s\n", optarg);
                if (NULL != optarg) {
                    from_db_path = optarg;
                }
                break;
            case 't':
                printf("ARGUMENT: t [start_time] = %s\n", optarg);
                if (NULL != optarg) {
                    strptime(optarg, "%Y-%m-%d-%H:%M:%S", &start_time);
                    start_time.tm_isdst = 0;
                    printf("start_time: %d-%d-%d %d:%d:%d\n", \
                        start_time.tm_year, start_time.tm_mon, start_time.tm_mday,\
                        start_time.tm_hour, start_time.tm_min, start_time.tm_sec);
                    break;
                }
            case 'x':
                printf("ARGUMENT: x [thread num] = %s\n", optarg);
                thread_num = atoi(optarg);
                break;
            case 'h':
            case '?':
                print_helper();
                return 0;
        }
    }


    printf("ARGUMENT SUMMARY:\n");
    printf("  - batch_size = %d\n", target_fv_size);
    printf("  - database_size = %ld Byte\n", db_size);
    printf("  - store_path = %s\n", to_db_path.empty()?"null":to_db_path.c_str());
    printf("  - restore_path = %s\n", from_db_path.empty()?"null":from_db_path.c_str());
    printf("  - loop = %d\n", loop_num);
    char start_time_buf[64];
    strftime(start_time_buf, 64, "%Y-%m-%d %H:%M:%S", &start_time);
    if (start_time.tm_year > 0) {
        printf("  - start_time = %s\n", start_time_buf);
    } else {
        printf("  - start_time = now\n");
    }
    printf("  - thread_num = %d\n", thread_num);


    char* db = NULL;

    if (!from_db_path.empty()) {
        db = create_db_from_file (from_db_path.c_str(), &db_size);
    } else {
        db = create_db_from_memory (db_size);
    }

    printf(" -- db_size = %ld\n", db_size);

    if (!to_db_path.empty()) {
        store_db_to_file (db, db_size, to_db_path.c_str());
    }

    
    char* target_fv = NULL;
    target_fv = create_target_fv_batch (target_fv_size);

    if (start_time.tm_year > 0) {
        wait_to_start(start_time);
    }


    ThreadArg targ;
    targ.loop_num = loop_num;
    targ.target_fv = target_fv;
    targ.target_fv_size = target_fv_size;
    targ.db = db;
    targ.db_size = db_size;

    std::vector<std::thread*> pool;

    for (i = 0; i < thread_num; i++) {
        std::thread *t = new std::thread (thread_func, i, &targ);
        pool.push_back (t);
    }

    for (auto &j : pool) {
        j->join();
    }

    // struct timeval t1, t2;
    // double time_used = 0.0;

    // gettimeofday (&t1, NULL);
    // for (i = 0; i < loop_num; i++) {
    //    printf(" --> LOOP: %d/%d starts.\n", i+1, loop_num);
    //    search_fv_batch (target_fv, target_fv_size, db, db_size);
    // }
    // gettimeofday (&t2, NULL);

    // time_used = (t2.tv_sec - t1.tv_sec) + (double) (t2.tv_usec - t1.tv_usec)/1000000.0;

    // overall_matching_amount = thread_num * loop_num * target_fv_size * (db_size/FV_LEN);
    // overall_throughput = overall_matching_amount / time_used;

    // printf("\n\nRESULT:\n");
    // printf(" ==> overall_matching_amount = thread_num * loop_num * fv_batch_size * db_size / FV_LEN = %d * %d * %d * %ld * %d = %ld\n", thread_num, loop_num, target_fv_size, db_size, FV_LEN, overall_matching_amount);
    // printf(" ==> time_used = %.2f sec\n", time_used);
    // printf(" ==> overall_throughput = %.2f times/sec\n", overall_throughput);

    free_db (db);
    free_target_fv_batch (target_fv);

    return 0;
}
