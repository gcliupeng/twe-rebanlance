#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include "main.h"
#include "dist.h"
#include <signal.h>  

rebance_server server; 
void logRaw(const char * function, int line, int level, const char * fmt, ...){
	if(level<logLeve){												
		return;														
	}																
	char msg[500];													
	int n;																	
	time_t cur;																		
	struct tm cur_tm = {0};										
	time(&cur);														
    localtime_r(&cur, &cur_tm);												
	switch(level){												
		case LOG_DEBUG:											
			n = sprintf(msg,"[DEBUG]");	
			break;						
		case LOG_NOTICE:										
			n = sprintf(msg,"[NOTICE]");
			break;						
		case LOG_WARNING:										
			n = sprintf(msg,"[WORNING]");
			break;						
		case LOG_ERROR:											
			n = sprintf(msg,"[ERROR]");
			break;									
	}																			
	n+=sprintf(msg+n,"%4d/%02d/%02d %02d:%02d:%02d  ",cur_tm.tm_year + 1900,
                     cur_tm.tm_mon + 1,							
                     cur_tm.tm_mday,							
                     cur_tm.tm_hour,							
                     cur_tm.tm_min,								
                     cur_tm.tm_sec);							
	va_list ap;															
    va_start(ap, fmt);											
    vsnprintf(msg+n, sizeof(msg)-n, fmt, ap);					
    va_end(ap);													
    fprintf(logfp,"%s Functon: %s Line: %d\n",msg,function,line);									\
    fflush(logfp);												
}																
												
void initLog(const char *log,int level){
	if(log == NULL){
		logfp = stdout;
	}else{
		logfp = fopen(log,"a");
		if(!logfp){
			logfp = stdout;
		}
	}
	logLeve = level;
}

void initConf(){
	if(!server.old_config){
		return;
	}
	if(!server.new_config){
		return;
	}
	switch (server.new_config->distType) {
		case DIST_KETAMA:
			ketama_dist(server.new_config);
	}
}

void initThreads(){
	//改版；对每一个old_config->servers，起一个进程；解决经常dump问题

	int count = array_n(server.old_config->servers);
	while(count){
		int r = fork();
		if(r<0){
			Log(LOG_ERROR,"fork error, errno:%d",errno);
			exit;
		}
		//child
		if(r ==0){
			array * servers = array_create(2,sizeof(server_conf));
			server_conf *sct = array_push(servers);
			server_conf *sctt = array_get(server.old_config->servers, count-1);
			*sct = *sctt;
			server.old_config->servers = servers;
			break;
		}else{
			//father
			count--;
			if(!count){
				exit(0);
			}
		}
	}

	int n = array_n(server.old_config->servers);
	int i;
	thread_contex * th;
	server_conf * sc;
	for (i = 0; i < n; ++i){
		sc = array_get(server.old_config->servers, i);
		th = malloc(sizeof(*th));
		if(!th){
			Log(LOG_ERROR,"malloc error");
			exit(1);
		}
		sc->contex = (struct thread_contex_s *)th;
		th->sc = sc;
		th->transfer_size = -1;
		pthread_mutex_init(&th->mutex,NULL);
		pthread_create(&th->pid,NULL,transferFromServer,th);
	}

	n = array_n(server.new_config->servers);
	for (i = 0; i < n; ++i){
		sc = array_get(server.new_config->servers,i);
		th = malloc(sizeof(*th));
		if(!th){
			Log(LOG_ERROR,"malloc error");
			exit(1);
		}
		sc->contex = (struct thread_contex_s *)th;
		th->sc = sc;
		pthread_mutex_init(&th->mutex,NULL);
		th->bufout = th->bufoutLast = NULL;
		pthread_create(&th->pid,NULL,outPutLoop,th);
	}
}
// int createThreadContex(dict * map,server_conf *sc){
// 	thread_contex *th = malloc(sizeof(*th));
// 	th->sc = sc;
// 	dictAdd(map,sc->name,sc->name_length,)
// }

void func(){
	Log(LOG_ERROR, "server close the connection error");
}

int main(int argc, char const *argv[])
{
	/* code */
	const char* oldConf = "old.yml";
	const char* newConf = "new.yml";
	//const char* log = "twe-rebanlance.log";
	server.logfile = "twe-rebanlance.log";
	server.loglevel = LOG_NOTICE;
	server.prefix = "";
	server.filter = "";
	int i;
	for (i = 1; i < argc; ++i){

		if(!strcmp(argv[i],"-o")){
			oldConf = argv[++i];
		}
		if(!strcmp(argv[i],"-n")){
			newConf = argv[++i];
		}
		if(!strcmp(argv[i],"-c")){
			loadConfig(argv[++i]);
		}
	}

	initLog(server.logfile,server.loglevel);
	server.old_config = loadFromFile(oldConf);
	server.new_config = loadFromFile(newConf);
	init_pool();
	if(!server.old_config || !server.new_config){
		exit(1);
	}
	Log(LOG_NOTICE, "config file parse ok ");
	
	/*dump
	*/
	//printf("filter is %s\n",server.filter);
	//printf("prefix is %s\n",server.prefix);
	// printf("the hash type is %d\n",server.old_config->hashType);
	// printf("the dist type is %d\n",server.old_config->distType);
	// printf("the auth is %s\n",server.old_config->auth);
	// for (int i = 0; i < array_n(server.old_config->servers); ++i)
	// {
	// 	server_conf * sc = array_get(server.old_config->servers, i);
	// 	printf("%x\n",sc );
	// 	printf("the server name is %s\n",sc->name );
	// 	printf("the port is %d\n",sc->port);
	// 	printf("the weight is %d\n",sc->weight );
	// 	/* code */
	// }
	// oldMap = createDict(20);
	// newMap = createDict(20);

	initConf();
	initThreads();

	//wait signal
	 struct sigaction act;  
     act.sa_handler = func;  
   	sigemptyset(&act.sa_mask); 
  	sigaction(SIGPIPE, &act, 0);

	sigset_t		set;
	sigemptyset(&set);
	sigaddset(&set,SIGPIPE);
    sigsuspend(&set);

	return 0;
}