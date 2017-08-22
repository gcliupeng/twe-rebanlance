#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <malloc.h>
#include <fcntl.h>
#include "network.h"
#include "config.h"
#include "main.h"
#include "ev.h"
#include "parse.h"
#include "dist.h"

extern rebance_server server;

int processMulti(thread_contex * th){
	int num = 0;
	long long ll;
	char *p;
	if(th->bucknum == -1){
		p = strstr(th->replicationBuf,"\r\n");
		
		if(!p){
			//not enough data
			return -1;
		}
		//printf("%s\n",p+2);
		string2ll(th->replicationBuf+1,p-(th->replicationBuf+1),&ll);
		th->bucknum = ll;
		//printf("%d\n", th->bucknum);
		th->replicationBufPos = p+2;
	}

	while(th->bucknum){

		if(th->lineSize == -1){
			p = strstr(th->replicationBufPos,"\r\n");
			if(!p){
				return -1;
			}
			string2ll(th->replicationBufPos+1,p-(th->replicationBufPos+1),&ll);
			th->replicationBufPos = p+2;
			th->lineSize = ll;
			//printf("%d\n",th->lineSize );
			th->step ++;
		}
			
			//p = strstr(th->replicationBufPos,"\r\n");
		if(th->replicationBufLast - th->replicationBufPos < th->lineSize+2){
			return -1;
		}
		if(th->step ==1){
			//printf("%s\n",th->replicationBufPos );
			if(strncmp(th->replicationBufPos,"PING",4)==0 || strncmp(th->replicationBufPos,"ping",4) ==0){
				th->type = REDIS_CMD_PING;
			}
			if(strncmp(th->replicationBufPos,"SELECT",6)==0 || strncmp(th->replicationBufPos,"select",6)==0){
				th->type = REDIS_CMD_SELECTDB;
			}
		}
		if(th->step == 2){
			th->key_length = th->lineSize;
			th->key = th->replicationBufPos;
		}
		th->replicationBufPos = th->replicationBufPos+th->lineSize+2;
		th->lineSize =-1;
		th->bucknum --;
	}
		return 1;
}

int processSingle(thread_contex * th){
	char * p = strstr(th->replicationBuf,"\r\n");
	if(!p){
		//not enough
		return -1;
	}
	th->replicationBufPos = p+2;
	return 1;
}
void resetState(thread_contex * th){
	int n;
	n = th->replicationBufLast-th->replicationBufPos;
	memcpy(th->replicationBuf,th->replicationBufPos,n);
	th->replicationBufLast =th->replicationBuf +n;
	th->replicationBufPos = th->replicationBuf;
	memset(th->replicationBufLast,0,th->replicationBufSize-n);
	th->bucknum = -1;
	th->lineSize = -1;
	th->inputMode = -1;
	th->type = -1;
	th->step = 0;
}
void  replicationWithServer(void * data){
	event *ev = data;
	thread_contex * th = ev->contex;
	int fd = ev->fd;
	int n , i;
	int left;
	int extra;
	while(1){
		left = th->replicationBufSize-(th->replicationBufLast - th->replicationBuf);
		if(left == 0){
			//large
			th->replicationBuf = realloc(th->replicationBuf,th->replicationBufSize*2);
			th->replicationBufPos = th->replicationBufLast = th->replicationBuf+th->replicationBufSize;
			th->replicationBufSize *=2;
		}
		n = read(fd, th->replicationBufLast ,left);
		if(n < 0){
			return;
		}
	 	th->replicationBufLast+=n;
		while(th->replicationBufPos != th->replicationBufLast){
			if(th->inputMode == -1){
				if(th->replicationBuf[0] == '*'){
					th->inputMode = 1;//multi line
				}else{ 
					th->inputMode = 0; //single line
				}
			}

			if(th->inputMode == 1){
				n = processMulti(th);
			}else{
				//single mode 实现有问题，但2.8版本已经不用single mode，后续版本待验证
				n = processSingle(th);
			}
			if(n == -1){
				//not enough data
				break;
			}
			// parse ok

			if(th->type == REDIS_CMD_PING || th->type == REDIS_CMD_SELECTDB){
				//printf("is ping or select\n");
				resetState(th);
				continue ;
			}
			
			//send to new server
			uint32_t hash = server_hash(server.new_config, th->key, th->key_length);
			int index = dispatch(server.new_config,hash);
			server_conf * from = th->sc;
			server_conf * to = array_get(server.new_config->servers,index);
			char t[1000];
			snprintf(t,th->key_length+1 ,"%s",th->key );
			Log(LOG_DEBUG, "the key %s, from %s:%d",t, from->pname,from->port);
			Log(LOG_DEBUG, "the key %s should goto %s:%d",t, to->pname, to->port);

			if(strcmp(from->pname,to->pname)==0 && from->port == to->port){
					//printf("the key from is same to %s\n",t);
				Log(LOG_DEBUG,"the key %s server not change ",t);
				resetState(th);
				continue;
			}

			//send to new
			buf_t * output = getBuf(th->replicationBufPos - th->replicationBuf);
			if(!output){
				//printf("getBuf error\n");
				Log(LOG_ERROR,"get buf error");
				exit(1);
			}
			
			memcpy(output->start, th->replicationBuf, th->replicationBufPos-th->replicationBuf);
            output->start[th->replicationBufPos-th->replicationBuf] = '\0';
            output->position = output->start+(th->replicationBufPos - th->replicationBuf);

			appendToOutBuf(to->contex, output);
			resetState(th);
		}
	}

}

int  saveRdb(thread_contex * th){
	int fd = th->fd;
	server_conf * sc = th->sc;
	//char fileName [100]={0};
	memset(th->rdbfile,0,100);
	sprintf(th->rdbfile,"rdb-%d-%p.rdb",getpid(),pthread_self());
	Log(LOG_NOTICE, "begin save the rdb from server %s:%d , the file is %s",sc->pname,sc->port ,th->rdbfile);
	int filefd = open(th->rdbfile,O_RDWR|O_CREAT,0644);
	if(filefd <0){
		return 0;
	}
	char buf[1024];
	int n;
	while(th->transfer_size){
		n = read(fd,buf,1024);
		write(filefd,buf,n);
		th->transfer_size-=n;
	}
	Log(LOG_NOTICE, "save the rdb from server %s:%d done, the file is %s",sc->pname,sc->port ,th->rdbfile);
	return 1;
}


void * transferFromServer(void * data){
	thread_contex * th = data;

	//create eventLoop;
	eventLoop * loop = eventLoopCreate();
	if(!loop){
		Log(LOG_ERROR ,"eventLoopCreate error");
		exit(1);
	}
	th->loop = loop;

	event * w =malloc(sizeof(*w));
	if(!w){
		Log(LOG_ERROR, "create event error");
		exit(1);
	}
	th->write = w;

	event * r =malloc(sizeof(*r));
	if(!w){
		Log(LOG_ERROR, "create event error");
		exit(1);
	}
	th->read = r;


	server_conf * sc = th->sc;
	//printf("%d\n",sc->port );
	th->fd = connetToServer(sc->port,sc->pname);
	if(th->fd <= 0){
		Log(LOG_ERROR, "can't connetToServer %s:%d",sc->pname,sc->port);
		exit(1);
	}

	Log(LOG_NOTICE, " connetToServer %s:%d ok ",sc->pname,sc->port);

	if(!sendSync(th)){
		Log(LOG_ERROR,"can't send sync to server %s:%p",sc->pname,sc->port);
		exit(1);
	}

	Log(LOG_NOTICE, "send sync to server %s:%d ok",sc->pname,sc->port);

	//th->fd = open("/home/work/redis-limited/dump.rdb4",O_RDWR);
	//th->fd = open("/home/work/twe-rebanlance/data",O_RDWR);

	if(!parseSize(th)){
		//printf("parse size error \n");
		Log(LOG_ERROR, "parse size error from server %s:%d",sc->pname,sc->port);
		exit(1);
	}
	Log(LOG_NOTICE, "parse size from server %s:%d ok, the size is %llu",sc->pname,sc->port ,th->transfer_size);

	if(!saveRdb(th)){
		Log(LOG_ERROR, "save the rdb error %s:%d",sc->pname,sc->port);
		exit(1);
	}
	
	int backfd = th->fd;
	th->fd = open(th->rdbfile,O_RDWR);
	if(!processHeader(th)){

		Log(LOG_ERROR, "parse header error from server %s:%d",sc->pname,sc->port);
		exit(1);
	}

	if(!parseRdb(th)){
		//printf("parse the rdb error");
		Log(LOG_ERROR, "parse rdb error from server %s:%d",sc->pname,sc->port);
		//Log(LOG_NOTICE, "redo with the server %s:%d in 10s ",sc->pname,sc->port);

		exit(1);
	}

	Log(LOG_NOTICE, "parse rdb from the master %s:%d done",sc->pname,sc->port);

	//sync done, next is relication 

	th->fd = backfd;
	// change to nonblocking
	nonBlock(th->fd);

	r->type = EVENT_READ;
	r->fd = th->fd;
	r->rcall = replicationWithServer;
	r->contex = th;
	addEvent(th->loop,r,EVENT_READ);

	//init
	th->replicationBufSize = 1024;
	th->replicationBuf = malloc(1024*sizeof(char));
	th->replicationBufLast = th->replicationBufPos = th->replicationBuf;
	th->bucknum = -1;
	th->lineSize = -1;
	th->inputMode = -1;
	th->step = 0;

	eventCycle(th->loop);
}


void  sendData(void * data){
	event *ev = data;
	thread_contex * th = ev->contex;
	int fd = ev->fd;
	int n;
	//get one buf
	pthread_mutex_lock(&th->mutex);
	buf_t * buf = th->bufout;
	//printf("%p \n",buf );
	//delEvent(th->loop ,ev, EVENT_WRITE);
	if(!buf){
		//printf("no buf\n");
		delEvent(th->loop ,ev, EVENT_WRITE);
		pthread_mutex_unlock(&th->mutex);
		return;
	} 
	//printf("come here\n");
	th->bufout = buf->next;
	pthread_mutex_unlock(&th->mutex);

	//send it
	buf->last = buf->position; 
	buf->position = buf->start;
	n = write(fd, buf->position, buf->last-buf->position);
	if(n == buf->last - buf->position){
		//printf("send %d\n", n);
		freeBuf(buf);
	}else{
		// put again
		pthread_mutex_lock(&th->mutex);
		buf->next = th->bufout;
		th->bufout = buf;
		pthread_mutex_unlock(&th->mutex);
	}
}


void * outPutLoop(void * data){
	thread_contex * th = data;

	server_conf * sc = th->sc;
	th->fd = connetToServer(sc->port,sc->pname);
	if(th->fd <= 0){
		Log(LOG_ERROR, "can't connetToServer %s:%d",sc->pname,sc->port);
		exit(1);
	}

	//create eventLoop;
	eventLoop * loop = eventLoopCreate();
	if(!loop){
		Log(LOG_ERROR, "eventLoopCreate error");
		exit(1);
	}
	th->loop = loop;

	event * w =malloc(sizeof(*w));
	if(!w){
		Log(LOG_ERROR, "create event error");
		exit(1);
	}
	th->write = w;

	event * r =malloc(sizeof(*r));
	if(!r){
		Log(LOG_ERROR, "create event error");
		exit(1);
	}
	th->read = r;
	w->type = EVENT_WRITE;
	w->fd = th->fd;
	w->wcall = sendData;
	w->contex = th;
	addEvent(th->loop,w,EVENT_WRITE);
	eventCycle(th->loop);
}