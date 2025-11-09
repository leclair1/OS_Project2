#define _GNU_SOURCE
#include "serial.h"

#include <pthread.h>
#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <limits.h>
#include <zlib.h>

/* --------------------------------------------------------------------------
   here is an outline of the plan
   1. we scan input_dir for .txt files and sort names in lex order
   2. we build a results table with one slot per file
   3. we push one job per file into a shared queue
   4. workers read and compress in parallel
   5. we write members in lex order using the starter writer
   note we keep final writes single threaded to avoid container issues
   -------------------------------------------------------------------------- */

/* ----------------------------- data models ------------------------------ */

typedef struct job {
    char *path;           // full path to disk file
    char *logical_name;   // name to store inside the zip
    int index;            // position in lex order
    struct job *next;     // next job in queue
} job_t;                    // job structure

typedef struct {
    unsigned char **out_bufs;  // one buffer per file
    size_t *out_sizes;         // compressed size per file
    int count;                // number of files
} results_t;                 // results table

/* ---------------------------- work queue core --------------------------- */

typedef struct {
    pthread_mutex_t mtx;        // mutex for queue operations
    pthread_cond_t cv_have;     // condition var for job availability
    pthread_cond_t cv_done; // condition var for queue completion
    job_t *head;             // head of the job queue
    job_t *tail;                // tail of the job queue
    int open;                // queue is open while producer pushes
    int active;                 // workers currently holding a job
    results_t *res;              // pointer to results table
} queue_t;                       // queue structure

// here we set up locks and flags
static void queue_init(queue_t *q, results_t *res) {
    pthread_mutex_init(&q->mtx, NULL);      // initialize mutex
    pthread_cond_init(&q->cv_have, NULL);   // initialize condition variable
    pthread_cond_init(&q->cv_done, NULL); // initialize condition variable
    q->head = q->tail = NULL;       // initialize head and tail to null
    q->head = q->tail = NULL;       // initialize head and tail to null
    q->open = 1;        // initialize open to 1
    q->active = 0;      // initialize active to 0
    q->res = res;    // store results table pointer
    q->res = res; // store results table pointer
}

// here we close the queue so workers stop waiting after drain
static void queue_close(queue_t *q) { 
    pthread_mutex_lock(&q->mtx); // lock mutex
    q->open = 0; // set open to 0 
    pthread_cond_broadcast(&q->cv_have); // broadcast condition variable
    pthread_mutex_unlock(&q->mtx); // unlock mutex
}

// now we push a job by appending and then signal the condition var
static void queue_push(queue_t *q, job_t *j) {
    j->next = NULL; // set next to null
    pthread_mutex_lock(&q->mtx); // lock mutex
    append_to_tail(&q->tail, j); // append to tail
    pthread_cond_signal(&q->cv_have); // signal condition variable
    pthread_mutex_unlock(&q->mtx); // unlock mutex
}
 
// this section a consumer pops a job or returns null if closed and empty
static job_t* queue_pop(queue_t *q) {
    pthread_mutex_lock(&q->mtx); // lock mutex
    while (q->head == NULL && q->open == 1) { // while no head and open
        pthread_cond_wait(&q->cv_have, &q->mtx); // waiting for condition variable
    }
    job_t *job = q->head; // head
    if (job) { // if job is not null
        q->head = job->next; // set head to next
        if (q->head == NULL) { // if head is null
            q->tail = NULL; // set tail to null
        }
        job->next = NULL; // set next to null
        q->active++; // increment active
    }
    pthread_mutex_unlock(&q->mtx); // unlock mutex
    return job; // return job or null
}

// here a consumer reports done so we can detect global completion
static void queue_task_done(queue_t *q) {
    pthread_mutex_lock(&q->mtx); // lock mutex
    q->active--; // decrement active
    // if closed and empty and active is zero then we signal cv_done
    if (q->open == 0 && q->head == NULL && q->active == 0) { // if open is zero and head is null and active is zero
        pthread_cond_signal(&q->cv_done); // signal condition variable
    }
    pthread_mutex_unlock(&q->mtx); // unlock mutex
}

// here the producer waits until queue is empty and no consumer is active
static void queue_wait_all(queue_t *q) {
    pthread_mutex_lock(&q->mtx); // lock mutex
    // while open or head not null or active not zero wait on cv_done
    while (q->open == 1 || q->head != NULL || q->active != 0) {
        pthread_cond_wait(&q->cv_done, &q->mtx); // waiting for condition variable
    }
    pthread_mutex_unlock(&q->mtx); // unlock mutex
}

/* ------------------------------ io helpers ------------------------------ */

// here we join dir and base name
static char* join_path(const char *dir, const char *base) {
    size_t dir_len = strlen(dir); // get length of dir
    size_t base_len = strlen(base); // get length of base
    size_t len = dir_len + 1 + base_len + 1; // now we calculate length of new path
    char *path = (char*)malloc(len); // and allocate memory for new path
    if (!path) { // but if allocation failed
        return NULL; // return null
    }
    strcpy(path, dir); // here we copy dir to path
    // this section we allocate buffer for dir plus optional slash plus base plus null
    if (dir[dir_len - 1] != '/') { // if dir does not end with slash
        path[dir_len] = '/'; // add slash
        dir_len++; // and increment dir_len
    }
    strcpy(path + dir_len, base); // copy base to path
    return path; // return new path
}

// here we read a whole file into memory
static int read_whole_file(const char *path, unsigned char **buf, size_t *len) {
    FILE *file = fopen(path, "rb"); // open file in read binary mode
    if (!file) { // if file is null
        return -1; // return -1
    }
    fseek(file, 0, SEEK_END); // seek to end of file
    long size = ftell(file); // get size of file
    rewind(file); // rewind file
    *buf = (unsigned char*)malloc(size); // allocate memory for buffer
    if (!*buf) { // if buffer is null
        fclose(file); // close file
        return -1; // return -1
    }

    // we need to malloc size bytes and free
    if (fread(*buf, 1, size, file) != size) { // so if read failed
        fclose(file); // close file
        free(*buf); // free buffer
        return -1; // and return -1
    }
    // finally t the end we set length and return 0
    fclose(file); // close file
    *len = size; // set length
    return 0; // return 0
}

/* ---------------------------- compression stub -------------------------- */
// here we deflate  buffer using zlib
static int deflate_buffer(const unsigned char *in, size_t in_len, unsigned char **out, size_t *out_len) {
    if (!out || !out_len) { // if output or output length is null
        return -1; // return -1
    }
    *out = NULL; // set output to null
    *out_len = 0; // set output length to 0
    if (in_len > (size_t)ULONG_MAX) { // if input length is greater than ULONG_MAX
        return -1; // return -1
    }

    uLong src_len = (uLong)in_len; // converting input length to uLong
    uLongf dst_len = compressBound(src_len); // getting compressed dir length
    if (dst_len == 0) { // if compressed dir length is zero...
        dst_len = 1; // then set dir length to 1
    }
    unsigned char *dst = (unsigned char*)malloc((size_t)dst_len); // allocate compressed buffer
    if (!dst) { // if compressed buffer is null
        return -1; // return -1
    }

    int zrc = compress2(dst, &dst_len, (const Bytef*)in, src_len, Z_BEST_SPEED); // compress input'
    // here we check if compression failed
    if (zrc != Z_OK) { // if compression failed
        free(dst); // free compressed buffer
        return -1; // return -1
    }
    unsigned char *tight = (unsigned char*)realloc(dst, (size_t)dst_len); // reallocate compressed buffer
    if (tight) { // if reallocation successful
        dst = tight; // set dst to tight
    }

    *out = dst; // set output to compressed buffer
    *out_len = (size_t)dst_len; // set output length
    return 0; // return 0
}

/* ------------------------------ worker/consumer code ----------------------------- */

typedef struct {
    queue_t *q;                   // store the queue pointer for this worker
} worker_arg_t;

/* here we free a single job and its owned fields */
static void free_job(job_t *j) {
    if (!j) return;            // check if the job is null before freeing
    free(j->path);                // free the memory used for the file path
    free(j->logical_name);        // and free the memory used for the logical name
    free(j);                   // as well as free the job struct 
}

/* here each worker keeps taking jobs until the queue returns null (closed and empty) */
static void* worker_main(void *arg) {
    worker_arg_t *wa = (worker_arg_t *)arg;   // making the argument the right type

    if (!wa || !wa->q) {          // checking if the argument or queue is missing
        return NULL;              // and if so.. we stop the thread early if no work is possible
    }

    queue_t *q = wa->q;       //  the queue pointer for convenience

    for (;;) {                  // looping queue forever until null
        job_t *j = queue_pop(q);  //  taking one job from the queue (may block)
        if (!j) {                 // here we check if the queue is empty and closed
            break;          // and if so, we break out because no more jobs
        }

        unsigned char *raw = NULL;     //  file's raw bytes
        size_t raw_len = 0;            // raw file size
        unsigned char *zip = NULL;   // the compressed data
        size_t zip_len = 0;         // and here we store the compressed size

        int read_ok = read_whole_file(j->path, &raw, &raw_len);   // reading the file

        if (read_ok == 0 && raw && raw_len > 0) {                 // thren we check if we check if read worked
            int def_ok = deflate_buffer(raw, raw_len, &zip, &zip_len);   // if yes, then compress the data

            if (def_ok == 0 && zip && zip_len > 0) {              // now if compression worked
                results_t *res = q->res;                          // then we get the shared results struct
                if (res && j->index >= 0 && (size_t)j->index < (size_t)res->count) { // and if the index is valid
                    res->out_bufs[j->index] = zip;                // then we store the compressed data
                    res->out_sizes[j->index] = zip_len;           // store the compressed size
                    zip = NULL;                                   // and we set zip to null so we don’t free it
                }
            }
        }

        free(raw);      // free the raw buffer we allocated
        free(zip);          //  free the zip buffer if it wasn’t stored
        queue_task_done(q);  // marking the job as finished in the queue
        free_job(j);        // here we free the job struct and its memory
    }
    return NULL;      // and then  we end the thread and return
}



/* ----------------------- directory scan and ordering -------------------- */

static int ends_with_txt(const char *s) {
    // todo check if s ends with .txt
   size_t len = strlen(s);
    if (len < 4) return 0;
    return (s[len-4] == '.' && s[len-3] == 't' && 
            s[len-2] == 'x' && s[len-1] == 't');
    return 0;
}

static int cmp_lex(const void *a, const void *b) {
    const char * const *pa = (const char * const *)a;
    const char * const *pb = (const char * const *)b;
    return strcmp(*pa, *pb);
}

// here we list .txt files in lex order and return a heap array
static char** list_txt_lex(const char *dir, int *out_count) {
    // todo open directory
    // todo collect names that end with .txt into a heap array
    // todo qsort with cmp_lex
    // todo set out_count and return names or null on error
    return NULL;
}

/* --------------------------- zip writer glue ---------------------------- */

// here we plug into the starter zip writer
static int zip_begin(const char *output_zip) {
    // todo call starter open function
    (void)output_zip;
    return 0;
}

static int zip_write_member(const char *logical_name,
                            const unsigned char *bytes, size_t nbytes) {
    // todo call starter helper to write one member
    (void)logical_name; (void)bytes; (void)nbytes;
    return 0;
}

static int zip_end(void) {
    // todo call starter close function
    return 0;
}

/* ----------------------------- public entry ----------------------------- */

static int clamp_threads(int requested) {
    if (requested < 1) requested = 1;
    if (requested > 19) requested = 19;
    return requested;
}

int run_parallel(const char *input_dir, const char *output_zip, int requested_threads) {
    // here we list names in lex order
   int count = 0;
    char **lex = list_txt_lex(input_dir, &count);
    if (!lex || count == 0) {
 // todo free lex if partially allocated
        if (lex) free(lex);
        return 0;
    }

    // here we make the results table
    results_t res = (results_t){0};
    res.count = count;
    res.out_bufs = (unsigned char**)calloc((size_t)count, sizeof(unsigned char*));
    res.out_sizes = (size_t*)calloc((size_t)count, sizeof(size_t));
    if (!res.out_bufs || !res.out_sizes) {
        // todo free res arrays and lex
        free(res.out_bufs);
        free(res.out_sizes);
        for (int i = 0; i < count; i++) free(lex[i]);
        free(lex);
        return -1;
        
    }

    // here we init the queue
    queue_t q;
    queue_init(&q, &res);

    // here we start worker threads
    int nthreads = clamp_threads(requested_threads);
    pthread_t *ths = (pthread_t*)malloc((size_t)nthreads * sizeof(pthread_t));
    worker_arg_t wa = { .q = &q };
    for (int i = 0; i < nthreads; ++i) {
        // todo create thread that runs worker_main with wa
      pthread_create(&ths[i], NULL, worker_main, &wa);
    }

    // here we enqueue jobs
    for (int i = 0; i < count; ++i) {
        // todo set j->logical_name to strdup of lex[i]
        // todo set j->path with join_path
        // todo set j->index
        // todo push into queue
          job_t *j = (job_t*)calloc(1, sizeof(job_t)); 
          j->logical_name = strdup(lex[i]);  
          j->path = join_path(input_dir, lex[i]); 
          j->index = i;
          queue_push(&q, j);
    }

    // here we close the queue and wait for all work to finish
      queue_close(&q); // todo queue_close
      queue_wait_all(&q);  // todo queue_wait_all

    // here we join all workers
    for (int i = 0; i < nthreads; ++i) {
        pthread_join(ths[i], NULL); // todo pthread_join
    }
    free(ths);

    // here we write members in lex order
    if (zip_begin(output_zip) != 0) {
        // todo decide error policy
    }
    for (int i = 0; i < count; ++i) {
        if (res.out_bufs[i] && res.out_sizes[i] > 0) {
            zip_write_member(lex[i], res.out_bufs[i], res.out_sizes[i]);
        } else {
            // todo decide how we handle failures
        }
    }
    zip_end();

    // here we clean up
    for (int i = 0; i < count; ++i) {
         free(lex[i]); // todo free lex[i]
        free(res.out_bufs[i]); // todo free res.out_bufs[i]
    }
    free(lex); // todo free lex
    free(res.out_bufs);
    free(res.out_sizes);// todo free res.out_bufs and res.out_sizes

    return 0;
}
