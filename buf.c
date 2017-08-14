#include "buf.h"
#include <pthread.h>

static buf_t *buf_pool;
static pthread_mutex_t pool_lock = PTHREAD_MUTEX_INITIALIZER;

void init_pool(){
	buf_pool = createBuf(1024);
}
void freeBuf(buf_t * b){
	b->position=b->last = b->start;
	pthread_mutex_lock(&pool_lock);
	b->next = buf_pool;
	if(buf_pool)
		buf_pool->pre = b;
	buf_pool = b;
	buf_pool->pre = NULL;
	pthread_mutex_unlock(&pool_lock);
}

buf_t * getBuf(int size){
	pthread_mutex_lock(&pool_lock);
	buf_t *buf = buf_pool;
	while(buf){
		if(bufSize(buf) >= size){
			if(buf->pre){
				buf->pre->next = buf->next;
			}
			if(buf->next){
				buf->next->pre = buf->pre;
			}
			if(buf == buf_pool){
				buf_pool = buf->next;
			}
			pthread_mutex_unlock(&pool_lock);
			buf->pre = buf->next = NULL;
			return buf;
		}
		buf = buf->next;
	}
	pthread_mutex_unlock(&pool_lock);
	return createBuf(size);
}