#ifndef INT_SET_H
#define INT_SET_H
#include <stdint.h>

typedef struct intset {
    uint32_t encoding;
    uint32_t length;
    int8_t contents[];
} intset;

uint32_t intsetLen(intset *is);

uint8_t intsetGet(intset *is, uint32_t pos, int64_t *value);

#endif