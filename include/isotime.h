#ifndef ISOTIME_H
#define ISOTIME_H

#include <stddef.h>
#include <stdint.h>

typedef enum {
    FASTEST = 0,
    BALANCED = 1,
    EXTREME_SPACE = 2
} ffi_compression_policy;

typedef struct {
    uint8_t* data;
    size_t len;
} isotime_buffer;

void* isotime_open(const char* wal_path, const char* cas_path, const uint8_t* enc_key, ffi_compression_policy policy);
void isotime_close(void* engine);
isotime_buffer isotime_get(void* engine, const uint8_t* key, size_t key_len);
void isotime_free_buffer(isotime_buffer buffer);

#endif
