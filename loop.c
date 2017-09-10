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
		p = strstr(th->replicationBufPos,"\r\n");
		
		if(!p){
			//not enough data
			return -1;
		}
		//printf("%s\n",p+2);
		string2ll(th->replicationBufPos+1,p-(th->replicationBufPos+1),&ll);
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
			/*
			if(strncmp(th->replicationBufPos,"flushall",8)==0 || strncmp(th->replicationBufPos,"FLUSHALL",8)==0){
				th->type = REDIS_CMD_SELECTDB;
			}*/
		}
		if(th->step == 2){
			th->key_length = th->lineSize;
			th->key = th->replicationBufPos;
			//加前缀，移动数据
			/*
			if(strlen(server.prefix) >0){
				int gap = lengthSize(th->lineSize)+3;

			
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
					int used2 = th->replicationBufPosPre - th->replicationBuf;
					th->replicationBuf = realloc(th->replicationBuf,th->replicationBufSize*2);
					th->replicationBufPos = th->replicationBuf +used;
					th->replicationBufLast = th->replicationBuf+usedL;
					th->replicationBufPosPre = th->replicationBuf+used2;
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
				*/
			//}

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
	th->replicationBufPosPre = th->replicationBufPos;
	th->bucknum = -1;
	th->lineSize = -1;
	th->inputMode = -1;
	th->type = -1;
	th->step = 0;
	th->key = NULL;
}
void moveBuf(thread_contex * th){
	int n1,n2,nk;
	n1 = th->replicationBufLast-th->replicationBufPosPre;
	n2 = th->replicationBufPos-th->replicationBufPosPre;
	if(th->key !=NULL){
		nk = th->key - th->replicationBufPosPre;
	}
	memmove(th->replicationBuf,th->replicationBufPosPre,n1);
	th->replicationBufLast =th->replicationBuf +n1;
	th->replicationBufPos = th->replicationBuf + n2;
	th->replicationBufPosPre = th->replicationBuf;
	if(th->key != NULL){
		th->key = th->replicationBuf + nk;
	}
	memset(th->replicationBufLast,0,th->replicationBufSize-n1);
}
void  replicationWithServer(void * data){
	event *ev = data;
	thread_contex * th = ev->contex;
	int fd = ev->fd;
	int n , i;
	int left;
	int extra;
	long readed = 0;
	static char t[1000];
	while(1){
		left = th->replicationBufSize-(th->replicationBufLast - th->replicationBuf);
		if(left == 0){
			if(th->replicationBufPos< th->replicationBuf + th->replicationBufSize/2){
				int used = th->replicationBufPos - th->replicationBuf;
				int used2 = th->replicationBufPosPre - th->replicationBuf;
				int nk;
				if(th->key != NULL){
					nk = th->key - th->replicationBuf;
				}
				th->replicationBuf = realloc(th->replicationBuf,th->replicationBufSize*2);
				th->replicationBufPos = th->replicationBuf +used;
				th->replicationBufPosPre = th->replicationBuf +used2;
				th->replicationBufLast = th->replicationBuf+th->replicationBufSize;
				if(th->key != NULL){
					th->key = th->replicationBuf + nk;
				}
				left = th->replicationBufSize;
				th->replicationBufSize *=2;
			}else{
				moveBuf(th);
				left = th->replicationBufSize-(th->replicationBufLast - th->replicationBuf);
			}
		}
		n = read(fd, th->replicationBufLast ,left);
		if(n < 0){
			return;
		}
		readed +=n;
	 	th->replicationBufLast+=n;
		while(th->replicationBufPos != th->replicationBufLast){
			if(th->inputMode == -1){
				if(th->replicationBufPos[0] == '*'){
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
			
			if(strlen(server.prefix) >0){
				int gap = lengthSize(th->key_length)+3;
				memset(t,0,1000);
				memcpy(t,server.prefix,strlen(server.prefix));
				memcpy(t+strlen(server.prefix),th->key,th->key_length);
			}else{
				memset(t,0,1000);
				memcpy(t,th->key,th->key_length);
			}
			
			//send to new server
			uint32_t hash = server_hash(server.new_config, t, th->key_length+strlen(server.prefix));
			int index = dispatch(server.new_config,hash);
			server_conf * from = th->sc;
			server_conf * to = array_get(server.new_config->servers,index);
			
			
			Log(LOG_DEBUG, "the key %s, from %s:%d",t, from->pname,from->port);
			Log(LOG_DEBUG, "the key %s should goto %s:%d",t, to->pname, to->port);
			if(th->processed %10000 ==0){
				Log(LOG_NOTICE, "processed %lld key  read %ld bytes from output buf from %s:%d",th->processed,readed, th->aoffile,from->pname,from->port);	
			}
			if(strcmp(from->pname,to->pname)==0 && from->port == to->port){
					//printf("the key from is same to %s\n",t);
				Log(LOG_DEBUG,"the key %s server not change ",t);
				resetState(th);
				continue;
			}

			//send to new
			buf_t * output = getBuf(th->replicationBufPos - th->replicationBufPosPre+10+strlen(server.prefix));
			if(!output){
				//printf("getBuf error\n");
				Log(LOG_ERROR,"get buf error");
				exit(1);
			}
			
			if(strlen(server.prefix) >0){
				char * beforKey = th->key - lengthSize(th->key_length) -2;
				long length,nn=0;

				length = beforKey-th->replicationBufPosPre;
				memcpy(output->start+nn, th->replicationBufPosPre, length);
				nn += length;
				
				length = sprintf(output->start+nn,"%d\r\n",th->key_length+strlen(server.prefix));
				nn += length;

				length = strlen(server.prefix);
				memcpy(output->start+nn,server.prefix,strlen(server.prefix));
				nn += length;

				length = th->replicationBufPos - th->key;
				memcpy(output->start +nn ,th->key,length);
				nn += length;
				output->position = output->start + nn;
			}else{
				memcpy(output->start, th->replicationBufPosPre, th->replicationBufPos-th->replicationBufPosPre);
				output->position = output->start+(th->replicationBufPos - th->replicationBufPosPre);
			}
			
            //output->start[th->replicationBufPos-th->replicationBufPosPre] = '\0';
            
            //printf("%s", output->start);

			appendToOutBuf(to->contex, output);
			resetState(th);
			th->processed++;
		}
	}

}

void replicationAof(thread_contex *th){
	int fd ;
	fd = open(th->aoffile,O_RDWR|O_CREAT,0644);
    if(fd <0){
    	Log(LOG_ERROR,"can't open the aof file , %s ,errno %d",th->aoffile,errno);
        return;
   	}
   	th->processed =0;
	//off_t currpos;
	//currpos = lseek(fd, 0, SEEK_SET);
	int n , i;
	int left;
	int extra;
	static char t[1000];
	while(1){
		left = th->replicationBufSize-(th->replicationBufLast - th->replicationBuf);
		if(left == 0){
			if(th->replicationBufPosPre< th->replicationBuf + th->replicationBufSize/2){
				int used = th->replicationBufPos - th->replicationBuf;
				int used2 = th->replicationBufPosPre - th->replicationBuf;
				int nk;
				if(th->key != NULL){
					nk = th->key - th->replicationBuf;
				}
				th->replicationBuf = realloc(th->replicationBuf,th->replicationBufSize*2);
				th->replicationBufPos = th->replicationBuf +used;
				th->replicationBufPosPre = th->replicationBuf +used2;
				th->replicationBufLast = th->replicationBuf+th->replicationBufSize;
				if(th->key != NULL){
					th->key = th->replicationBuf + nk;
				}
				left = th->replicationBufSize;
				th->replicationBufSize *=2;
			}else{
				moveBuf(th);
				left = th->replicationBufSize-(th->replicationBufLast - th->replicationBuf);
			}
		}
		//printf("left %d\n",left );
		n = read(fd, th->replicationBufLast ,left);
		//Log(LOG_NOTICE, "aof read prcess %d byte",n);
		if(n <= 0){
			//printf("aof read done %d\n",errno);
			//Log(LOG_NOTICE, "aof read done , %s",th->aoffile);
			return;
		}
	 	th->replicationBufLast+=n;
		while(th->replicationBufPos != th->replicationBufLast){
			if(th->inputMode == -1){
				if(th->replicationBufPos[0] == '*'){
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
			if(th->key == NULL){
				Log(LOG_NOTICE,"%p",th->key);
				Log(LOG_NOTICE,"%s",th->replicationBufPosPre);
			}
			if(strlen(server.prefix) >0){
				memset(t,0,1000);
				memcpy(t,server.prefix,strlen(server.prefix));
				memcpy(t+strlen(server.prefix),th->key,th->key_length);
			}else{
				memset(t,0,1000);
				memcpy(t,th->key,th->key_length);
			}

			//send to new server
			
			uint32_t hash = server_hash(server.new_config, t, th->key_length+strlen(server.prefix));
			int index = dispatch(server.new_config,hash);
			server_conf * from = th->sc;
			server_conf * to = array_get(server.new_config->servers,index);
			if(th->processed %1000 ==0){
					Log(LOG_NOTICE, "processed %lld key in the aof file %s , from %s:%d",th->processed,th->aoffile,from->pname,from->port);	
			}
			
			//t[th->key_length]='\0';

			//snprintf(t,th->key_length+1 ,"%s",th->key );
			
			Log(LOG_DEBUG, "the key %s, from %s:%d",t, from->pname,from->port);
			Log(LOG_DEBUG, "the key %s should goto %s:%d",t, to->pname, to->port);
			if(th->processed %1000 ==0){
					Log(LOG_NOTICE, "processed %lld key in the aof file %s , from %s:%d",th->processed,th->aoffile,from->pname,from->port);	
			}
			if(strcmp(from->pname,to->pname)==0 && from->port == to->port){
					//printf("the key from is same to %s\n",t);
				Log(LOG_DEBUG,"the key %s server not change ",t);
				resetState(th);
				continue;
			}
			
			//send to new
			buf_t * output = getBuf(th->replicationBufPos - th->replicationBufPosPre+10);
			if(!output){
				//printf("getBuf error\n");
				Log(LOG_ERROR,"get buf error");
				exit(1);
			}
			
			if(strlen(server.prefix) >0){
				char * beforKey = th->key - lengthSize(th->key_length) -2;
				long length,nn=0;

				length = beforKey-th->replicationBufPosPre;
				memcpy(output->start+nn, th->replicationBufPosPre, length);
				nn += length;
				
				length = sprintf(output->start+nn,"%d\r\n",th->key_length+strlen(server.prefix));
				nn += length;

				length = strlen(server.prefix);
				memcpy(output->start+nn,server.prefix,strlen(server.prefix));
				nn += length;

				length = th->replicationBufPos - th->key;
				memcpy(output->start +nn ,th->key,length);
				nn += length;
				output->position = output->start + nn;
			}else{
				memcpy(output->start, th->replicationBufPosPre, th->replicationBufPos-th->replicationBufPosPre);
				output->position = output->start+(th->replicationBufPos - th->replicationBufPosPre);
			}

			appendToOutBuf(to->contex, output);
			
			resetState(th);
			th->processed ++;
		}
	}

}

int  saveRdb(thread_contex * th){
	int fd = th->fd;
	server_conf * sc = th->sc;
	memset(th->rdbfile,0,100);
	sprintf(th->rdbfile,"rdb-%s-%d.rdb",sc->pname,sc->port);
	Log(LOG_NOTICE, "begin save the rdb from server %s:%d , the file is %s",sc->pname,sc->port ,th->rdbfile);
	int filefd = open(th->rdbfile,O_RDWR|O_CREAT,0644);
	if(filefd <0){
		return 0;
	}
	char buf[1024];
	long  n ,left;
	th->transfer_read = 0;
	while(th->transfer_size > th->transfer_read){
		left = th->transfer_size - th->transfer_read;
		if(left > 1024){
			left = 1024;
		}
		n = read(fd,buf,left);
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
	Log(LOG_NOTICE, "begin parse the rdb from server %s:%d, the rdbfile is %s",th->sc->pname,th->sc->port,th->rdbfile);
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

	Log(LOG_NOTICE, "parse the rdb from server %s:%d done , the rdbfile is %s ,processed %d",sc->pname,sc->port,th->rdbfile,th->processed);
	close(th->rdbfd);
	unlink(th->rdbfile);
}

void * saveAofThread(void *data){
	thread_contex * th = data;
	Log(LOG_NOTICE, "create the aof file  from server %s:%d , the file is %s",th->sc->pname,th->sc->port ,th->aoffile);
	int filefd = open(th->aoffile,O_RDWR|O_CREAT,0644);
    if(filefd <0){
    	Log(LOG_ERROR,"can't open the aof file , %s ,errno %d",th->aoffile,errno);
        return 0;
   	}
   	int loop =0;
   	long sum = 0;
	while(!stop){
		char buf[1024*100];
    	int n;
    	n = read(th->fd,buf,1024*100);
        if(n>0){
            write(filefd,buf,n);
            sum+=n;
            if(loop %1000 ==0){
            	Log(LOG_NOTICE, "write into aof file %s , %lld byte ",th->aoffile, sum);
            }
            loop++;
        }
    }
    close(filefd);
    aof_alive = 0;
    Log(LOG_NOTICE, "write the aof file %s done ,total  %lld byte ",th->aoffile, sum);
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

	Log(LOG_NOTICE, "connetToServer %s:%d ok ",sc->pname,sc->port);

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
    sprintf(th->aoffile,"aof-%s-%d.aof",sc->pname,sc->port);
	//sprintf(th->aoffile,"aof-127.0.0.1-6379.aof");
	pthread_t saveAofthread;
	aof_alive = 1;
	pthread_create(&saveAofthread,NULL,saveAofThread,th);
	parseRdbThread(th);
	//sleep(50);
	//处理保存的aof文件
	stop = 1;
	pthread_mutex_lock(&sync_mutex);

	while(aof_alive == 1){
		pthread_cond_wait(&sync_cond,&sync_mutex);
	}

	pthread_mutex_unlock(&sync_mutex);
	
	//sprintf(th->aoffile,"aof-127.0.0.1-6379.aof");
	th->replicationBufSize = 1024*1024;
	th->replicationBuf = malloc(1024*1024*sizeof(char));
	th->replicationBufLast = th->replicationBufPos = th->replicationBuf;
	th->bucknum = -1;
	th->lineSize = -1;
	th->inputMode = -1;
	th->step = 0;
	th->key = NULL;
	Log(LOG_NOTICE, "begin process the aof file from server %s:%d , the file is %s",sc->pname,sc->port ,th->aoffile);
	replicationAof(th);
	//close(th->aoffd);
	Log(LOG_NOTICE, "process the aof file  done server %s:%d , the file is %s , processed %lld",sc->pname,sc->port ,th->aoffile,th->processed);
	unlink(th->aoffile);
	//检查是否server已经把连接关闭
	struct tcp_info info; 
  	int len=sizeof(info); 
  	getsockopt(th->fd, IPPROTO_TCP, TCP_INFO, &info, (socklen_t *)&len); 
  	if(info.tcpi_state != TCP_ESTABLISHED){
  		Log(LOG_ERROR, "socket closed %s:%d",sc->pname,sc->port);
  		//todo 
  	}
  	Log(LOG_NOTICE, "begin process the server output buf %s:%d ",sc->pname,sc->port);
	nonBlock(th->fd);
	th->processed = 0;
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
	// buf->last = buf->position; 
	// buf->position = buf->start;
	//Log(LOG_DEBUG,"need write %d",buf->last-buf->position);
	if(buf->last-buf->position ==0){
		freeBuf(buf);
		return;
	}
	n = write(fd, buf->position, buf->last-buf->position);
	//Log(LOG_DEBUG,"after write %d",buf->last-buf->position);
	if(n == buf->last - buf->position){
		//printf("send %d\n", n);
		freeBuf(buf);
	}else{
		buf->position += n;
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

	Log(LOG_DEBUG, "checkConnect ");
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