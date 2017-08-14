#ifndef CONFIG_H
#define CONFIG_H

#include "array.h"
#include <string.h>

struct thread_contex_s;
enum {
    HASH_ONE_AT_A_TIME = 0,
    HASH_MD5,
    HASH_CRC16,
    HASH_CRC32,
    HASH_CRC32A,
    HASH_FNV1_64,
    HASH_FNV1A_64,
    HASH_FNV1_32,
    HASH_FNV1A_32,
    HASH_HSIEH,
    HASH_MURMUR,
    HASH_JENKINS
};

enum {
    DIST_KETAMA = 0,
    DIST_MODULA,
    DIST_RANDOM
};

typedef struct{
	char * name;
	int  kind;
}hashType;

typedef struct{
	char * name;
	int  kind;
}distributionType;

typedef struct{
    uint32_t value;
    int index;
}continuum;

typedef struct{
	int hashType;
	int distType;
	array * servers;
    continuum * con;
    int ncontinuum;
}config;

typedef struct{
	int weight;
	char pname[100];
	int pname_length;
	char name[100];
    int name_length;
    int port;
    struct thread_contex_s * contex;
}server_conf;

config * loadFromFile(const char * file);
void loadConfig(const char * file);

static int toNumber(char *p ,char *q){
    int value = 0;

    for (; p < q ; p++) {
        if (*p < '0' || *p > '9') {
            return -1;
        }

        value = value * 10 + (*p - '0');
    }

    if (value < 0) {
        return -1;
    }
    return value;
}
#endif