
#include <stdlib.h>
#include "array.h"

struct array *
array_create(uint32_t n, size_t size)
{
    struct array *a;

    a = malloc(sizeof(*a));
    if (a == NULL) {
        return NULL;
    }

    a->elem = malloc(n * size);
    if (a->elem == NULL) {
        free(a);
        return NULL;
    }

    a->nelem = 0;
    a->size = size;
    a->nalloc = n;

    return a;
}

void
array_destroy(struct array *a)
{
    array_deinit(a);
    free(a);
}

int
array_init(struct array *a, uint32_t n, size_t size)
{

    a->elem = malloc(n * size);
    if (a->elem == NULL) {
        return 0;
    }

    a->nelem = 0;
    a->size = size;
    a->nalloc = n;

    return 1;
}

void
array_deinit(struct array *a)
{

    if (a->elem != NULL) {
        free(a->elem);
    }
}

uint32_t
array_idx(struct array *a, void *elem)
{
    uint8_t *p, *q;
    uint32_t off, idx;

    p = a->elem;
    q = elem;
    off = (uint32_t)(q - p);

    idx = off / (uint32_t)a->size;

    return idx;
}

void *
array_push(struct array *a)
{
    void *elem, *new;
    size_t size;

    if (a->nelem == a->nalloc) {

        /* the array is full; allocate new array */
        size = a->size * a->nalloc;
        new =  realloc(a->elem, 2 * size);
        if (new == NULL) {
            return NULL;
        }

        a->elem = new;
        a->nalloc *= 2;
    }

    elem = (uint8_t *)a->elem + a->size * a->nelem;
    a->nelem++;

    return elem;
}

void *
array_pop(struct array *a)
{
    void *elem;

    a->nelem--;
    elem = (uint8_t *)a->elem + a->size * a->nelem;

    return elem;
}

void *
array_get(struct array *a, uint32_t idx)
{
    void *elem;

    elem = (uint8_t *)a->elem + (a->size * idx);

    return elem;
}

void *
array_top(struct array *a)
{

    return array_get(a, a->nelem - 1);
}

int array_each(struct array *a, array_each_t func, void *data)
{
    uint32_t i, nelem;

    for (i = 0, nelem = array_n(a); i < nelem; i++) {
        void *elem = array_get(a, i);
        int status;

        status = func(elem, data);
        if (status != 1) {
            return status;
        }
    }

    return 1;
}