#ifndef BUF_H
#define BUF_H
#include <stdlib.h>

struct buf_s{
	char * start;
	char * end;
	char * position;
	char * last;
	struct buf_s * next;
	struct buf_s * pre;
};

typedef struct buf_s buf_t;

static buf_t * createBuf(int size){
	buf_t * b =malloc(sizeof(*b));
	if(!b){
		return NULL;
	}
	char *p = malloc(sizeof(char)*size);
	if(!p){
		free(b);
		return NULL;
	}
	b->start=b->position=b->last=p;
	b->end = p+size*sizeof(char);
	return b;
}

static int bufSize(buf_t *b){
	if(!b){
		return 0;
	}
	//printf("%d \n",b->end - b->start );
	return b->end - b->start;
}

static int bufLength(buf_t *b){
	if(!b){
		return 0;
	}
	return b->last-b->position;
}

static int bufAvailable(buf_t *b){
	if(!b){
		return 0;
	}
	return b->end - b->last;
} 
void freeBuf(buf_t * b);
buf_t * getBuf(int size);
#endif