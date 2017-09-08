#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <malloc.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <errno.h>

#include "network.h"
#include "config.h"
#include "main.h"
#include "ev.h"
#include "parse.h"
#include "dist.h"

extern rebance_server server;

static pthread_cond_t sync_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t sync_mutex = PTHREAD_MUTEX_INITIALIZER;
static int sync_ok;
static int stop;
static int aof_alive;
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
			//加前缀，移动数据
			if(strlen(server.prefix) >0){
				//
				int gap = lengthSize(th->lineSize+strlen(server.prefix)) - lengthSize(th->lineSize);
				int back = lengthSize(th->lineSize)+2;

				int left = th->replicationBufSize-(th->replicationBufLast - th->replicationBuf);
				if(left >= strlen(server.prefix) + gap){
					//直接移动
					//printf("gap %d\n", gap);

					memmove(th->replicationBufPos+strlen(server.prefix)+gap,th->replicationBufPos,th->replicationBufLast-th->replicationBufPos);
					th->replicationBufPos += gap;
					sprintf(th->replicationBufPos-back-gap,"%d\r\n",th->lineSize+strlen(server.prefix));
					memcpy(th->replicationBufPos,server.prefix,strlen(server.prefix));
				}else{
					int used = th->replicationBufPos - th->replicationBuf;
					int usedL = th->replicationBufLast - th->replicationBuf;
					th->replicationBuf = realloc(th->replicationBuf,th->replicationBufSize*2);
					th->replicationBufPos = th->replicationBuf +used;
					th->replicationBufLast = th->replicationBuf+usedL;
					th->replicationBufSize *=2;
					memmove(th->replicationBufPos+strlen(server.prefix)+gap,th->replicationBufPos,th->replicationBufLast-th->replicationBufPos);
					th->replicationBufPos += gap;
					sprintf(th->replicationBufPos-back-gap,"%d\r\n",th->lineSize+strlen(server.prefix));
					memcpy(th->replicationBufPos,server.prefix,strlen(server.prefix));
				}
				th->key_length+=strlen(server.prefix);
				th->key = th->replicationBufPos;
				th->replicationBufPos += strlen(server.prefix);
				th->replicationBufLast+=( strlen(server.prefix)+gap);
			}

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
	memmove(th->replicationBuf,th->replicationBufPos,n);
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
	char t[1000];
	while(1){
		left = th->replicationBufSize-(th->replicationBufLast - th->replicationBuf);
		if(left == 0){
			//large
			int used = th->replicationBufPos - th->replicationBuf;
			th->replicationBuf = realloc(th->replicationBuf,th->replicationBufSize*2);
			th->replicationBufPos = th->replicationBuf +used;
			th->replicationBufLast = th->replicationBuf+th->replicationBufSize;
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
			
			memset(t,0,1000);
			memcpy(t,th->key,th->key_length);
			Log(LOG_NOTICE, "the key %s, from %s:%d",t, from->pname,from->port);
			Log(LOG_NOTICE, "the key %s should goto %s:%d",t, to->pname, to->port);

			if(strcmp(from->pname,to->pname)==0 && from->port == to->port){
					//printf("the key from is same to %s\n",t);
				Log(LOG_DEBUG,"the key %s server not change ",t);
				resetState(th);
				continue;
			}

			//send to new
			buf_t * output = getBuf(th->replicationBufPos - th->replicationBuf+10);
			if(!output){
				//printf("getBuf error\n");
				Log(LOG_ERROR,"get buf error");
				exit(1);
			}
			
			memcpy(output->start, th->replicationBuf, th->replicationBufPos-th->replicationBuf);
            output->start[th->replicationBufPos-th->replicationBuf] = '\0';
            output->position = output->start+(th->replicationBufPos - th->replicationBuf);
            //printf("%s", output->start);

			appendToOutBuf(to->contex, output);
			resetState(th);
		}
	}

}

void replicationAof(thread_contex *th){
	int fd ;
	fd = open(th->aoffile,O_RDWR|O_CREAT,0644);
    if(fd <0){
        return;
   	}

	//off_t currpos;
	//currpos = lseek(fd, 0, SEEK_SET);
	int n , i;
	int left;
	int extra;
	char t[1000];
	while(1){
		left = th->replicationBufSize-(th->replicationBufLast - th->replicationBuf);
		if(left == 0){
			//large
			int used = th->replicationBufPos - th->replicationBuf;
			th->replicationBuf = realloc(th->replicationBuf,th->replicationBufSize*2);
			th->replicationBufPos = th->replicationBuf +used;
			th->replicationBufLast = th->replicationBuf+th->replicationBufSize;
			th->replicationBufSize *=2;
		}
		//printf("left %d\n",left );
		n = read(fd, th->replicationBufLast ,left);
		Log(LOG_NOTICE, "aof read prcess %d byte",n);
		//printf("read %d\n",n);
		//printf("data %s", th->replicationBufPos);
		if(n <= 0){
			//printf("aof read done %d\n",errno);
			Log(LOG_NOTICE, "aof read done ,%s",th->aoffile);
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
			
			memset(t,0,1000);
			memcpy(t,th->key,th->key_length);
			//snprintf(t,th->key_length+1 ,"%s",th->key );
			
			//Log(LOG_NOTICE, "the key %s, from %s:%d",t, from->pname,from->port);
			//Log(LOG_NOTICE, "the key %s should goto %s:%d",t, to->pname, to->port);

			if(strcmp(from->pname,to->pname)==0 && from->port == to->port){
					//printf("the key from is same to %s\n",t);
				Log(LOG_DEBUG,"the key %s server not change ",t);
				resetState(th);
				continue;
			}
			
			//send to new
			buf_t * output = getBuf(th->replicationBufPos - th->replicationBuf+10);
			if(!output){
				//printf("getBuf error\n");
				Log(LOG_ERROR,"get buf error");
				exit(1);
			}
			
			memcpy(output->start, th->replicationBuf, th->replicationBufPos-th->replicationBuf);
            output->start[th->replicationBufPos-th->replicationBuf] = '\0';
            output->position = output->start+(th->replicationBufPos - th->replicationBuf);
            //printf("%s", output->start);

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
	th->transfer_read = 0;
	while(th->transfer_size > th->transfer_read){
		n = read(fd,buf,1024);
		if(n ==0){
			Log(LOG_ERROR, "socket closed %s:%d",sc->pname,sc->port);
			//todo
		}
		write(filefd,buf,n);
		th->transfer_read += n;
	}
	Log(LOG_NOTICE, "save the rdb from server %s:%d done, the file is %s",sc->pname,sc->port ,th->rdbfile);
	close(filefd);
	return 1;
}

void * parseRdbThread(void *data){
	thread_contex *th = data;
	server_conf * sc = th->sc;
	th->rdbfd = open(th->rdbfile,O_RDWR);
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

	Log(LOG_NOTICE, "parse rdb from the master %s:%d done,processed %ld",sc->pname,sc->port,th->processed);
	close(th->rdbfd);
	unlink(th->rdbfile);
}

void * saveAofThread(void *data){
	aof_alive = 1;
	thread_contex * th = data;
	int filefd = open(th->aoffile,O_RDWR|O_CREAT,0644);
    if(filefd <0){
        return 0;
   	}
	while(!stop){
		char buf[1024*100];
    	int n;
    	n = read(th->fd,buf,1024*100);
        if(n>0){
            write(filefd,buf,n);
            Log(LOG_NOTICE, "write aof file %s %d byte ",th->aoffile, n);
        }
    }
    close(filefd);
    aof_alive = 0;
    pthread_mutex_lock(&sync_mutex);
    pthread_cond_broadcast(&sync_cond);
    pthread_mutex_unlock(&sync_mutex);
}

void * transferFromServer(void * data){
	thread_contex * th = data;
	stop = 0;
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

	//auth
	if(strlen(server.old_config->auth)>0){
		char auth[100];
		int n;
		n = sprintf(auth,"*2\r\n$4\r\nauth\r\n$%d\r\n%s\r\n",strlen(server.old_config->auth),server.old_config->auth);
		auth[n] = '\0';
		if(!sendToServer(th->fd,auth,strlen(auth))){
			Log(LOG_ERROR,"can't send auth:%s to server %s:%p",server.old_config->auth, sc->pname,sc->port);
			exit(1);
		}

		//read +OK\r\n
		if(readBytes(th->fd,auth,5)==0){
			Log(LOG_ERROR,"can't read auth:%s response, server %s:%p",server.old_config->auth, sc->pname,sc->port);
			exit(1);
		}
	}
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

	//create aof
	memset(th->aoffile,0,100);
    sprintf(th->aoffile,"aof-%d-%p.aof",getpid(),pthread_self());
    //sprintf(th->aoffile,"aof-14092-0x7f8f15189700.aof");
    //th->aoffile = "aof-14092-0x7f8f15189700.aof";
    Log(LOG_NOTICE, "create the aof file  from server %s:%d , the file is %s",sc->pname,sc->port ,th->aoffile);
    // int filefd = open(th->aoffile,O_RDWR|O_CREAT,0644);
    // if(filefd <0){
    //     return 0;
   	// }
   	//th->aoffd = filefd;
	
	//同步
	/*pthread_mutex_lock(&sync_mutex);
	sync_ok++;
	if(sync_ok == array_n(server.old_config->servers)){
		pthread_cond_broadcast(&sync_cond);
	}
	while(sync_ok < array_n(server.old_config->servers)){
		pthread_cond_wait(&sync_cond,&sync_mutex);
	}
	pthread_mutex_unlock(&sync_mutex);
	*/

	pthread_t saveAofthread;
	
	//pthread_create(&rdbthread,NULL,parseRdbThread,th);
	pthread_create(&saveAofthread,NULL,saveAofThread,th);
	sleep(10);
	//parseRdbThread(th);
	//处理保存的aof文件
	//init
	//sprintf(th->aoffile,"aof-14243-0x7fcce1185700.aof");
	//缺少同步
	stop = 1;
	pthread_mutex_lock(&sync_mutex);

	while(aof_alive == 1){
		pthread_cond_wait(&sync_cond,&sync_mutex);
	}

	pthread_mutex_unlock(&sync_mutex);

	th->replicationBufSize = 1024*1024;
	th->replicationBuf = malloc(1024*1024*sizeof(char));
	th->replicationBufLast = th->replicationBufPos = th->replicationBuf;
	th->bucknum = -1;
	th->lineSize = -1;
	th->inputMode = -1;
	th->step = 0;
	//printf("sdlfkjsaldfkjsalfjsalfkjsadlfjlaskjfdkl\n");
	Log(LOG_NOTICE, "begin process the aof file server %s:%d , the file is %s",sc->pname,sc->port ,th->aoffile);
	replicationAof(th);
	close(th->aoffd);
	Log(LOG_NOTICE, "process the aof file  done server %s:%d , the file is %s",sc->pname,sc->port ,th->aoffile);
	//unlink(th->aoffile);
	//检查是否server已经把连接关闭
	struct tcp_info info; 
  	int len=sizeof(info); 
  	getsockopt(th->fd, IPPROTO_TCP, TCP_INFO, &info, (socklen_t *)&len); 
  	if(info.tcpi_state != TCP_ESTABLISHED){
  		Log(LOG_ERROR, "socket closed %s:%d",sc->pname,sc->port);
  		//todo 
  	}
	nonBlock(th->fd);
	r->type = EVENT_READ;
	r->fd = th->fd;
	r->rcall = replicationWithServer;
	r->contex = th;
	/*th->replicationBufSize = 1024*1024;
	th->replicationBuf = malloc(1024*1024*sizeof(char));
	th->replicationBufLast = th->replicationBufPos = th->replicationBuf;
	th->bucknum = -1;
	th->lineSize = -1;
	th->inputMode = -1;
	th->step = 0;
	*/
	addEvent(th->loop,r,EVENT_READ);

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

void reconnect(thread_contex * th){
	server_conf * sc = th->sc;
	th->fd = connetToServer(sc->port,sc->pname);
	if(th->fd <= 0){
		Log(LOG_ERROR, "can't connetToServer %s:%d",sc->pname,sc->port);
		//exit(1);
		return;
	}

	//auth
	if(strlen(server.new_config->auth)>0){
		char auth[100];
		int n;
		n = sprintf(auth,"*2\r\n$4\r\nauth\r\n$%d\r\n%s\r\n",strlen(server.new_config->auth),server.new_config->auth);
		auth[n] = '\0';
		if(!sendToServer(th->fd,auth,strlen(auth))){
			Log(LOG_ERROR,"can't send auth:%s to server %s:%p",server.new_config->auth, sc->pname,sc->port);
			//exit(1);
			return;
		}
		//read +OK\r\n
		if(readBytes(th->fd,auth,5)==0){
			Log(LOG_ERROR,"can't read auth:%s response, server %s:%p",server.new_config->auth, sc->pname,sc->port);
			//exit(1);
			return;
		}
	}

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
	
}


void checkConnect(void * data){
	event *ev = data;
	thread_contex * th = ev->contex;
	int fd  = th->fd;
	// char tmp[10];
	// int n = read(fd,tmp,10);
	// if(n ==0){
	// 	Log(LOG_NOTICE, "closed");
	// }
	struct tcp_info info; 
  	int len=sizeof(info); 
  	getsockopt(th->fd, IPPROTO_TCP, TCP_INFO, &info, (socklen_t *)&len); 
  	if(info.tcpi_state != TCP_ESTABLISHED){
  		Log(LOG_ERROR, "socket closed %s:%d",th->sc->pname,th->sc->port);
  		//todo 
  		reconnect(th);
  	}

	Log(LOG_NOTICE, "checkConnect ");
	ev->type  = EVENT_TIMEOUT;
	ev->tcall = checkConnect;
	ev->timeout = 1000;
	addEvent(th->loop,ev,EVENT_TIMEOUT);
}


void * outPutLoop(void * data){
	thread_contex * th = data;

	server_conf * sc = th->sc;
	th->fd = connetToServer(sc->port,sc->pname);
	if(th->fd <= 0){
		Log(LOG_ERROR, "can't connetToServer %s:%d",sc->pname,sc->port);
		exit(1);
	}

	//auth
	if(strlen(server.new_config->auth)>0){
		char auth[100];
		int n;
		n = sprintf(auth,"*2\r\n$4\r\nauth\r\n$%d\r\n%s\r\n",strlen(server.new_config->auth),server.new_config->auth);
		auth[n] = '\0';
		if(!sendToServer(th->fd,auth,strlen(auth))){
			Log(LOG_ERROR,"can't send auth:%s to server %s:%p",server.new_config->auth, sc->pname,sc->port);
			exit(1);
		}
		//read +OK\r\n
		if(readBytes(th->fd,auth,5)==0){
			Log(LOG_ERROR,"can't read auth:%s response, server %s:%p",server.new_config->auth, sc->pname,sc->port);
			exit(1);
		}
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

	//每10秒钟检查一下是否连接
	event * t =malloc(sizeof(*t));
	if(!t){
		Log(LOG_ERROR, "create event error");
		exit(1);
	}
	t->contex = th;
	t->type  = EVENT_TIMEOUT;
	t->tcall = checkConnect;
	t->timeout = 1000;
	addEvent(th->loop,t,EVENT_TIMEOUT);
	eventCycle(th->loop);
}