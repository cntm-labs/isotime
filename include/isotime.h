#ifndef ISOTIME_H
#define ISOTIME_H

#include <stddef.h>
#include <stdint.h>

typedef enum {
    ISOTIME_OK = 0,
    ISOTIME_ERR_IO = 1,
    ISOTIME_ERR_NOT_FOUND = 2,
    ISOTIME_ERR_INVALID_QUERY = 3,
    ISOTIME_ERR_INTERNAL = 99
} isotime_status;

typedef enum {
    FASTEST = 0,
    BALANCED = 1,
    EXTREME_SPACE = 2
} ffi_compression_policy;

typedef struct {
    uint8_t* data;
    size_t len;
} isotime_buffer;

typedef struct {
    const uint8_t* key_data;
    size_t key_len;
    const uint8_t* val_data;
    size_t val_len;
} isotime_entry;

void* isotime_open(const char* wal_path, const char* cas_path, const uint8_t* enc_key, ffi_compression_policy policy);
void isotime_close(void* engine);
void isotime_put(void* engine, const uint8_t* key, size_t key_len, const uint8_t* val, size_t val_len);
isotime_buffer isotime_get(void* engine, const uint8_t* key, size_t key_len);
void isotime_free_buffer(isotime_buffer buffer);

// Maintenance API
int isotime_delete(void* engine, const uint8_t* key, size_t key_len);
int isotime_flush(void* engine);
int isotime_run_maintenance(void* engine);
int isotime_run_cas_gc(void* engine);

// Query API
void* isotime_query_new(void* engine);
void isotime_query_tag(void* query, const char* tag);
void isotime_query_range(void* query, double min, double max);
void isotime_query_after(void* query, const uint64_t* clock_ptr, size_t clock_len);
void* isotime_query_execute(void* query);

isotime_entry isotime_result_next(void* result_set);
void isotime_result_free(void* result_set);

#endif
