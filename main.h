#ifndef MAIN_H
#define MAIN_H

#include "config.h"
#include "buf.h"
#include "ev.h"
#include "struct.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <pthread.h>

#define REDIS_RUN_ID_SIZE 40

enum {
	NEED_HEAD = 0,
	NEED_TYPE,
	NEED_EXPIRE,
	NEED_EXPIREM,
	NEED_KEY,


};

typedef struct{
	config * old_config;
	config * new_config;
	char * prefix;
	char * filter;
	char * logfile;
	int loglevel;
	//aeEventLoop *el;
}rebance_server;

#ifndef EVENT_T
typedef struct eventLoop eventLoop;
typedef struct event event;
#define EVENT_T
#endif

struct thread_contex_s{
	server_conf * sc;
	pthread_t pid;
	pthread_mutex_t mutex;
	int fd;
	int rdbfd;
	char rdbfile[100];
	char * key;
	rvalue * value;
	long processed;
	int bucknum;
	buf_t * bufout;
	buf_t * bufoutLast;
	
	eventLoop *loop;
	event *read;
	event *write;
	long transfer_size;
	long transfer_read;
	int usemark;
	int version;
	int close;

	long key_length;
	long value_length;
	int type;
	int buffed;
	time_t expiretime;
	long long expiretimeM;

	int inputMode;
	int step;
	char * replicationBuf;
	int replicationBufSize;
	char * replicationBufPos;
	char * replicationBufLast;
	int lineSize;
};

#ifndef THREAD_T
typedef struct thread_contex_s thread_contex;
#define THREAD_T
#endif

void * transferFromServer(void *arg);
void * outPutLoop(void *arg);
void init_pool();
void logRaw(const char * function, int line, int level, const char * fmt, ...);

FILE *logfp;
int logLeve;
enum {
	LOG_DEBUG = 0,
	LOG_NOTICE,
	LOG_WARNING,
	LOG_ERROR
};

#define Log(level,format, ...)  logRaw(__FUNCTION__, __LINE__, level, format, ## __VA_ARGS__)
void initConf();
#endif