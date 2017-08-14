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
			server.logfile = p+8;
			server.logfile[strlen(server.logfile)-1]=0;
			if(server.logfile[strlen(server.logfile)] == '\r'){
				server.logfile[strlen(server.logfile)-1]=0;
			}
		}
		p=strstr(buf,"loglevel=");
		if(p!=NULL){
			if(strncmp(p+9,"DEBUG",5)==0){
				server.loglevel = LOG_DEBUG;
			};
			if(strncmp(p+9,"NOTICE",6)==0){
				server.loglevel = LOG_NOTICE;
			};
			if(strncmp(p+9,"WARNING",7)==0){
				server.loglevel = LOG_WARNING;
			};
			if(strncmp(p+9,"ERROR",5)==0){
				server.loglevel = LOG_ERROR;
			};
		}
		p = strstr(buf,"filter=");
		if(p !=NULL){
			server.filter = p+7;
			server.filter[strlen(server.filter)-1]=0;
			if(server.filter[strlen(server.filter)] == '\r'){
				server.filter[strlen(server.filter)-1]=0;
			}
		}
		p=strstr(buf,"prefix=");
		if(p !=NULL){
			server.prefix = p+7;
			server.prefix[strlen(server.prefix)-1]=0;
			if(server.prefix[strlen(server.prefix)] == '\r'){
				server.prefix[strlen(server.prefix)-1]=0;
			}
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