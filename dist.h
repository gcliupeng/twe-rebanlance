#ifndef DIST_H
#define DIST_H 
#include "config.h"

int ketama_dist(config *conf);
uint32_t server_hash(config *conf, uint8_t *key, uint32_t keylen);
uint32_t dispatch(config * sc, uint32_t hash);
#endif