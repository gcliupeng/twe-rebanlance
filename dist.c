#include <stdio.h>
#include <inttypes.h>
#include <stdlib.h>
#include <math.h>
#include "config.h"
#include "main.h"
#include "hash.h"

#define KETAMA_CONTINUUM_ADDITION   10  /* # extra slots to build into continuum */
#define KETAMA_POINTS_PER_SERVER    160 /* 40 points per hash */
#define KETAMA_MAX_HOSTLEN          86

void md5_signature(unsigned char *key, unsigned long length, unsigned char *result);
static uint32_t
ketama_hash(const char *key, size_t key_length, uint32_t alignment)
{
    unsigned char results[16];

    md5_signature((unsigned char*)key, key_length, results);

    return ((uint32_t) (results[3 + alignment * 4] & 0xFF) << 24)
        | ((uint32_t) (results[2 + alignment * 4] & 0xFF) << 16)
        | ((uint32_t) (results[1 + alignment * 4] & 0xFF) << 8)
        | (results[0 + alignment * 4] & 0xFF);
}

static int
ketama_item_cmp(const void *t1, const void *t2)
{
    const continuum *ct1 = t1, *ct2 = t2;

    if (ct1->value == ct2->value) {
        return 0;
    } else if (ct1->value > ct2->value) {
        return 1;
    } else {
        return -1;
    }
}
int ketama_dist(config *conf)
{
    uint32_t nserver;             /* # server - live and dead */
    uint32_t pointer_per_server;  /* pointers per server proportional to weight */
    uint32_t pointer_per_hash;    /* pointers per hash */
    uint32_t pointer_counter;     /* # pointers on continuum */
    uint32_t pointer_index;       /* pointer index */
    uint32_t points_per_server;   /* points per server */
    uint32_t continuum_index;     /* continuum index */
    uint32_t continuum_addition;  /* extra space in the continuum */
    uint32_t server_index;        /* server index */
    uint32_t value;               /* continuum value */
    uint32_t total_weight;        /* total live server weight */

    nserver = array_n(conf->servers);
    total_weight = 0;

    for (server_index = 0; server_index < nserver; server_index++) {
         server_conf *server = array_get(conf->servers, server_index);
            total_weight += server->weight;
    }

    continuum *continuum;
    uint32_t nserver_continuum = nserver + KETAMA_CONTINUUM_ADDITION;
    uint32_t ncontinuum = nserver_continuum * KETAMA_POINTS_PER_SERVER;

    continuum = malloc(sizeof(*continuum) * ncontinuum);
    if (continuum == NULL) {
        Log(LOG_ERROR,"malloc error");
        return 0;
    }

    conf->con = continuum;

    continuum_index = 0;
    pointer_counter = 0;
    for (server_index = 0; server_index < nserver; server_index++) {
        server_conf *server;
        float pct;

        server = array_get(conf->servers, server_index);

        pct = (float)server->weight / (float)total_weight;
        pointer_per_server = (uint32_t) ((floorf((float) (pct * KETAMA_POINTS_PER_SERVER / 4 * (float)nserver + 0.0000000001))) * 4);
        pointer_per_hash = 4;

        for (pointer_index = 1;
             pointer_index <= pointer_per_server / pointer_per_hash;
             pointer_index++) {

            char host[KETAMA_MAX_HOSTLEN]= "";
            size_t hostlen;
            uint32_t x;

            hostlen = snprintf(host, KETAMA_MAX_HOSTLEN, "%.*s-%u",
                               server->name_length, server->name,
                               pointer_index - 1);
            //printf("%s\n",host );
            for (x = 0; x < pointer_per_hash; x++) {
                value = ketama_hash(host, hostlen, x);
                conf->con[continuum_index].index = server_index;
                conf->con[continuum_index++].value = value;
            }
        }
        pointer_counter += pointer_per_server;
    }

    conf->ncontinuum = pointer_counter;
    qsort(conf->con, pointer_counter, sizeof(* conf->con),ketama_item_cmp);

    int i=0;
    for (int i = 0; i < 10 && i< pointer_counter; ++i)
    {
        //printf("index is %d , value is %u\n",conf->con[i].index,conf->con[i].value);
    }
}

uint32_t
ketama_dispatch(continuum *con, uint32_t ncontinuum, uint32_t hash)
{
    continuum * begin;
    continuum * end;
    continuum * left;
    continuum * right;
    continuum * middle;

    begin = left = con;
    end = right = con + ncontinuum;

    while (left < right) {
        middle = left + (right - left) / 2;
        if (middle->value < hash) {
          left = middle + 1;
        } else {
          right = middle;
        }
    }

    if (right == end) {
        right = begin;
    }

    return right->index;
 }

uint32_t dispatch(config * sc, uint32_t hash){
    switch(sc->distType){
        case DIST_KETAMA:
            return ketama_dispatch(sc->con, sc->ncontinuum , hash);
    }
}

uint32_t server_hash(config *conf, uint8_t *key, uint32_t keylen)
{
    if (array_n(conf->servers) == 1) {
        return 0;
    }

    if (keylen == 0) {
        return 0;
    }
    switch(conf->hashType){
        case HASH_FNV1A_64:
            return  hash_fnv1a_64(key,keylen);
    }
}
