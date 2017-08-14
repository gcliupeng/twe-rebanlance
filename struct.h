#ifndef STRUCT_H
#define STRUCT_H

#include <stdio.h>
#include <stdlib.h>

struct rlistset{ // list or set
	char * str;
	struct rlistset * next;
	struct rlistset * last;
};

struct rzset{
	double score;
	char * str;
	struct rzset * next;
	struct rzset * last;
};

struct rhash{
	char * field;
	char * value;
	struct rhash * next;
	struct rhash * last;
};

union rvalue{
	char * str; // string
	struct rlistset * listset;
	struct rzset * zset;
	struct rhash * hash;
};

typedef union rvalue rvalue;

struct rlistset * newListSet();
struct rlistset * listSetAdd(struct rlistset * l);
struct rzset * newZset();
struct rzset * zsetAdd(struct rzset * z);
struct rhash * newHash();
struct rhash * hashAdd(struct rhash * h);
void freeHash(struct rhash * h);
void freeListSet(struct rlistset * l);
void freeZset(struct rzset * z);

#undef isinf
#define isinf(x) \
     __extension__ ({ __typeof (x) __x_i = (x); \
     __builtin_expect(!isnan(__x_i) && !isfinite(__x_i), 0); })
     
#endif