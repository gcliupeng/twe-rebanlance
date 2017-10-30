#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include "main.h"

extern rebance_server server; 

hashType hashArray[]={
	{"one_at_a_time",HASH_ONE_AT_A_TIME},
	{"md5",HASH_MD5},
	{"crc16",HASH_CRC16},
	{"crc32",HASH_CRC32},
	{"crc32a",HASH_CRC32A},
	{"fnv1_64",HASH_FNV1_64},
	{"fnv1a_64",HASH_FNV1A_64},
	{"fnv1_32",HASH_FNV1_32},
	{"fnv1a_32",HASH_FNV1A_32},
	{"hsieh",HASH_HSIEH},
	{"murmur",HASH_MURMUR},
	{"jenkins",HASH_JENKINS}
};

distributionType distrArray[] ={
	{"ketama",DIST_KETAMA},
	{"modula",DIST_MODULA},
	{"random",DIST_RANDOM}
};

void loadConfig(const char * file){
	if(file == NULL){
		return;
	}
	char buf[201];
	FILE *fp = fopen(file,"r");
	if(!fp){
		Log(LOG_WARNING, "the log file can't open %s","error",file);
		return;
	}
	char *p;
	while(fgets(buf,201,fp) != NULL){
		p = strstr(buf,"logfile=");
		if(p!=NULL){
			p+=8;
			while(*p==' ')p++;
			server.logfile = p;
			int i=0;
			while(*(p+i) !='\r' && *(p+i) !='\n' && *(p+i) !='\0') i++;
			server.logfile[i]=0;
			char * c = malloc(strlen(server.logfile)+1);
			memcpy(c,server.logfile,strlen(server.logfile));
			server.logfile = c;
		}
		p=strstr(buf,"loglevel=");
		if(p!=NULL){
			p+=9;
			while(*p==' ')p++;
			if(strncmp(p,"DEBUG",5)==0){
				server.loglevel = LOG_DEBUG;
			};
			if(strncmp(p,"NOTICE",6)==0){
				server.loglevel = LOG_NOTICE;
			};
			if(strncmp(p,"WARNING",7)==0){
				server.loglevel = LOG_WARNING;
			};
			if(strncmp(p,"ERROR",5)==0){
				server.loglevel = LOG_ERROR;
			};
		}
		p = strstr(buf,"filter=");
		if(p !=NULL){
			p+=7;
			while(*p==' ')p++;
			server.filter = p;
			int i=0;
			while(*(p+i) !='\r' && *(p+i) !='\n' && *(p+i) !='\0') i++;
			server.filter[i]=0;
			char * c = malloc(strlen(server.filter)+1);
			memcpy(c,server.filter,strlen(server.filter));
			server.filter = c;
		}
		p=strstr(buf,"prefix=");
		if(p !=NULL){
			p+=7;
			while(*p==' ')p++;
			server.prefix = p;
			int i=0;
			while(*(p+i) !='\r' && *(p+i) !='\n' && *(p+i) !='\0') i++;
			server.prefix[i]=0;
			char * c = malloc(strlen(server.prefix)+1);
			memcpy(c,server.prefix,strlen(server.prefix));
			server.prefix = c;
		}
		p=strstr(buf,"removePrefix=");
		if(p !=NULL){
			p+=13;
			while(*p==' ')p++;
			server.removePre = p;
			int i=0;
			while(*(p+i) !='\r' && *(p+i) !='\n' && *(p+i) !='\0') i++;
			server.removePre[i]=0;
			char * c = malloc(strlen(server.removePre)+1);
			memcpy(c,server.removePre,strlen(server.removePre));
			server.removePre = c;
		}
	}
}

config * loadFromFile(const char * file){
	if(file == NULL){
		return NULL;
	}
	char buf[2001];
	FILE *fp = fopen(file,"r");
	if(!fp){
		Log(LOG_ERROR, "the log file can't open %s",file);
		return NULL;
	}
	config * c = malloc(sizeof(*c));
	c->servers = array_create(20,sizeof(server_conf));
	memset(c->auth,0,100);

	int hashT = -1;
	int distT = -1;
	int isServer = 0;
	while(fgets(buf,2001,fp) != NULL){
		//printf("%d\n",strlen(buf));
		char *p;
		char *q;
		char *port_b,*port_e;
		if(isServer){
			p = buf;
			while((*p==' '||*p=='-')&&*p!='\n'&&*p!='\r'&&*p!=0)p++;
			if(*p=='\n'||*p==0||*p=='\r'){
				continue;
			}
			server_conf *sc = array_push(c->servers);
			q = p;
			while(*q!=':'&&*q!='\r'&&*q!='\n')q++;
			if(*q=='\r'||*q=='\n'){
				Log(LOG_ERROR, "server block not ok %s,file %s",buf,file);
				return NULL;
			}
			strncpy(sc->pname,p,q-p);
			sc->pname_length = q-p;
			sc->name[q-p] = '\0';
			p=++q;
			while(*q!=':'&&*q!='\r'&&*q!='\n')q++;
			if(*q=='\r'||*q=='\n'){
				Log(LOG_ERROR, "server block not ok %s,file %s",buf , file);
				return NULL;
			}
			sc->port = toNumber(p,q);
			if(sc->port <=0){
				Log(LOG_ERROR, "server port not ok %s,file %s",buf , file);
				return NULL;
			}
			port_b = p;
			port_e = q;
			p = ++q;
			while(*q!=' '&&*q!='\r'&&*q!='\n')q++;
			if(*q=='\r'||*q=='\n'){
				Log(LOG_ERROR, "server block not ok %s ,file %s",buf,file);
				return NULL;
			}
			sc->weight = toNumber(p,q);
			if(sc->weight <=0){
				Log(LOG_ERROR, "server weight not ok %s,file %s",buf , file);
				return NULL;
			}
			//continue;
			while((*q==' ')&&*q!='\r'&&*q!='\n'&&*q!='\0')q++;
			if(*q=='\r'||*q=='\n'||*q=='\0'){
				strncpy(sc->name,sc->pname,sc->pname_length);
				sc->name[sc->pname_length]=':';
				strncpy(sc->name+sc->pname_length+1,port_b,port_e- port_b);
				sc->name_length = sc->pname_length+1+port_e - port_b;
				sc->name[sc->name_length] = '\0';
				continue;
			}else{
				p = q;
				//continue;
				while(*q!=' '&& *q!='\r'&&*q!='\n'&&*q!='\0') q++;
				strncpy(sc->name,p,q-p);
				sc->name_length = q-p;
				continue;
			}
		}
		p = strstr(buf,"hash:");
		if(p){
			p+=5;
			while(*p==' ')p++;
			q=p;
			while(*q!='\r'&&*q!='\n'&&*q!=' ')q++;
			*q='\0';
			hashType * h =hashArray;
			for (; h->name; h++)
			{
				if(!strcmp(h->name,p)){
					hashT = h->kind;
					break;
				}
			}
			if(hashT == -1){
				Log(LOG_ERROR, "the hash type unknown %s , file %s",p,file);
				return NULL;
			}else{
				continue;
			}
		}

		p = strstr(buf,"redis_auth:");
		if(p){
			p+=11;
			while(*p==' ')p++;
			q=p;
			while(*q!='\r'&&*q!='\n'&&*q!=' ')q++;
			*q='\0';
			memcpy(c->auth,p,strlen(p));
		}

		p = strstr(buf,"distribution:");
		if(p){
			p+=13;
			while(*p==' ')p++;
			q=p;
			while(*q!='\r'&&*q!='\n'&&*q!=' ')q++;
			*q='\0';
			distributionType * h =distrArray;
			for (; h->name; h++)
			{
				if(!strcmp(h->name,p)){
					distT = h->kind;
					break;
				}
			}
			if(distT == -1){
				Log(LOG_ERROR, "the distribution type unknown %s ,file %s",p,file);
				return NULL;
			}else{
				continue;
			}
		}

		p = strstr(buf,"servers:");
		if(p){
			isServer = 1;
		}
	}
	if(hashT == -1 ||distT == -1||array_n(c->servers)==0){
		return NULL;
	}else{
		c->hashType = hashT;
		c->distType = distT;
		return c;
	}
}