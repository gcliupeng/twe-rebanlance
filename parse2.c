#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <limits.h>
#include <math.h>
#include <sys/time.h>
#include <float.h>
#include <pthread.h>

#include "main.h"
#include "config.h"
#include "network.h"
#include "parse.h"
#include "lzf.h"
#include "struct.h"
#include "zipmap.h"
#include "ziplist.h"
#include "intset.h"

uint32_t server_hash(config *conf, uint8_t *key, uint32_t keylen);
uint32_t dispatch(config * sc, uint32_t hash);
extern rebance_server server;

int formatStr(char *p,char * str){
	sprintf(p,"$%ld\r\n%s\r\n",strlen(str),str);
} 
int formatDouble(char *p , double d){
    char dbuf[128], sbuf[128];
    int dlen, slen;
    if (isinf(d)) {
        if(d > 0)
        	return sprintf(p,"inf");
        else
        	return sprintf(p,"-inf");//"inf" : "-inf");
    } else {
        dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
        return sprintf(p,"$%d\r\n%s\r\n",dlen,dbuf);
    }
}

int lengthSize(long long length){
	int n = 0;
	while(length){
		n++;
		length/=10;
	}
	return n;
}
int doubleSize(double d){
    char dbuf[128], sbuf[128];
    int dlen, slen;
    if (isinf(d)) {
        if(d > 0)
        	return  3;
        else
        	return 4;//"inf" : "-inf");
    } else {
        dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
        return dlen;
        //slen = snprintf(sbuf,sizeof(sbuf),"$%d\r\n%s\r\n",dlen,dbuf);
        //addReplyString(c,sbuf,slen);
    }
}
int sdsll2str(char *s, long long value) {
    char *p, aux;
    unsigned long long v;
    size_t l;

    /* Generate the string representation, this method produces
     * an reversed string. */
    v = (value < 0) ? -value : value;
    p = s;
    do {
        *p++ = '0'+(v%10);
        v /= 10;
    } while(v);
    if (value < 0) *p++ = '-';

    /* Compute length and add null term. */
    l = p-s;
    *p = '\0';

    /* Reverse the string. */
    p--;
    while(s < p) {
        aux = *s;
        *s = *p;
        *p = aux;
        s++;
        p--;
    }
    return l;
}

int readBytes(int fd, char * p,int max){
	int n ,m =0;
	while(max >0){
		n = read(fd, p+m, max);
		if(n == 0){
			return 0;
		}
		m+=n;
		max -=n;
	}
	return m;
}

int readLine(int fd, char * ptr, int max){
    int nread = 0;
    while(max) {
        char c;

        if (read(fd,&c,1) == -1) {
        	//printf("read -1 !!\n");
        	return -1;
        }
        //printf("%c\n", c);
        if (c == '\n') {
            *ptr = '\0';
            if (nread && *(ptr-1) == '\r') *(ptr-1) = '\0';
            return nread;
        } else {
            *ptr++ = c;
            *ptr = '\0';
            nread++;
        }
        max--;
    }
    return nread;
}

int sendSync(thread_contex * th){
    
	int fd = th->fd;
	char *sync = "*1\r\n$4\r\nsync\r\n";
	if(!sendToServer(th->fd,sync,strlen(sync))){
		return 0;
	}
	return 1;
}

int parseSize(thread_contex *th){
	static char eofmark[REDIS_RUN_ID_SIZE];
	static char lastbytes[REDIS_RUN_ID_SIZE];

	char tmp[1024];
	int fd = th->fd;
	char *buf = tmp;

	if(readLine(fd,buf,1024) ==-1){
		return 0;
	}
	if (buf[0] == '-') {
		Log(LOG_ERROR, "MASTER %s:%d aborted replication : %s",th->sc->pname,th->sc->port ,buf+1);
        return 0;
    } else if (buf[0] == '\0') {
        while(1){
            if(readLine(fd,buf,1024) ==-1){
                break;
            }
            if(buf[0]!='\0'){
                break;
            }
        }
    }
    if (buf[0] != '$') {
    	Log(LOG_ERROR, "Bad protocol from MASTER %s:%d, the first byte is not '$' (we received '%d')",th->sc->pname,th->sc->port, buf[0]);
    	return 0;
    }
    if (strncmp(buf+1,"EOF:",4) == 0 && strlen(buf+5) >= REDIS_RUN_ID_SIZE) {
    	th->usemark = 1;
        memcpy(eofmark,buf+5,REDIS_RUN_ID_SIZE);
        memset(lastbytes,0,REDIS_RUN_ID_SIZE);
        th->transfer_size = 0;
        Log(LOG_NOTICE,"MASTER %s:%d <-> SLAVE sync: receiving streamed RDB",th->sc->pname,th->sc->port);
    } else{
        th->usemark = 0;
        th->transfer_size = strtol(buf+1,NULL,10);
        Log(LOG_NOTICE,"MASTER %s:%d <-> SLAVE sync: receiving %lld bytes from master",th->sc->pname,th->sc->port,
        	(long long) th->transfer_size);
        }
 	return 1;
}

int processHeader(thread_contex * th) {
    int fd = th->fd;
    char buf[10] = "_________";
    int dump_version;

    if (!readBytes(fd,buf, 9)) {
        Log(LOG_ERROR, "Cannot read header, server %s:%d",th->sc->pname,th->sc->port);
        return 0;
    }

    /* expect the first 5 bytes to equal REDIS */
    if (memcmp(buf,"REDIS",5) != 0) {
        Log(LOG_ERROR, "Wrong signature in header, server %s:%d",th->sc->pname,th->sc->port);
        return 0;
    }

    dump_version = (int)strtol(buf + 5, NULL, 10);
    if (dump_version < 1 || dump_version > 8) {
        Log(LOG_ERROR, "Unknown RDB format version: %d\n", dump_version);
        return 0;
    }
    th->version = dump_version;
    Log(LOG_NOTICE, "redis version is %d, server %s:%d",dump_version,th->sc->pname,th->sc->port);
    th->transfer_size-=9;
    return dump_version;
}

int checkType(unsigned char t) {
    return
        (t >= REDIS_HASH_ZIPMAP && t <= REDIS_HASH_ZIPLIST) ||
        t <= REDIS_HASH ||
        t >= REDIS_EXPIRETIME_MS;
}

int loadType(int fd) {
    /* this byte needs to qualify as type */
    unsigned char t;
    if (!readBytes(fd,&t, 1)) {
        Log(LOG_ERROR, "cannot read type");
        return -1;
    }

    if(!checkType(t)){
    	Log(LOG_ERROR, "Unknown type, %d",t);
    	return -1;
    }
    //Log(LOG_NOTICE, "type  %d\n", t);
    return t;
}

int processTime(int fd,int type,time_t * expiretime, long long * expiretimeM) {

    int i;
    if(type == REDIS_EXPIRETIME_MS){
    	if(!readBytes(fd,(char*)expiretimeM,8))
    		return 0;
    	else{

    		return 8;
        }
    }else{
    	if(!readBytes(fd,(char*)expiretime,4))
    		return 0;
    	else
    		return 4;
    }
}

uint32_t loadLength(int fd, int *isencoded) {
    unsigned char buf[2];
    uint32_t len;
    int type;

    if (isencoded) *isencoded = 0;
    if (!readBytes(fd, buf, 1)) return REDIS_RDB_LENERR;
    type = (buf[0] & 0xC0) >> 6;
    if (type == REDIS_RDB_6BITLEN) {
        /* Read a 6 bit len */
        return buf[0] & 0x3F;
    } else if (type == REDIS_RDB_ENCVAL) {
        /* Read a 6 bit len encoding type */
        if (isencoded) *isencoded = 1;
        return buf[0] & 0x3F;
    } else if (type == REDIS_RDB_14BITLEN) {
        /* Read a 14 bit len */
        if (!readBytes(fd,buf+1,1)) return REDIS_RDB_LENERR;
        return ((buf[0] & 0x3F) << 8) | buf[1];
    } else {
        /* Read a 32 bit len */
        if (!readBytes(fd, (char*)&len, 4)) return REDIS_RDB_LENERR;
        return (unsigned int)ntohl(len);
    }
}

char* loadLzfStringObject(int fd) {
    unsigned int slen, clen;
    char *c, *s;

    if ((clen = loadLength(fd,NULL)) == REDIS_RDB_LENERR) return NULL;
    if ((slen = loadLength(fd,NULL)) == REDIS_RDB_LENERR) return NULL;

    c = malloc(clen);

    if (!readBytes(fd,c, clen)) {
        free(c);
        return NULL;
    }


    s = malloc(slen+1);

    if (lzf_decompress(c,clen,s,slen) == 0) {
        free(c); free(s);
        return NULL;
    }
    s[slen] = '\0';

    free(c);
    return s;
}

char *loadIntegerObject(int fd, int enctype) {
    unsigned char enc[4];
    long long val;

    if (enctype == REDIS_RDB_ENC_INT8) {
        uint8_t v;
        if (!readBytes(fd, enc, 1)) return NULL;
        v = enc[0];
        val = (int8_t)v;
    } else if (enctype == REDIS_RDB_ENC_INT16) {
        uint16_t v;
        if (!readBytes(fd, enc, 2)) return NULL;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
    } else if (enctype == REDIS_RDB_ENC_INT32) {
        uint32_t v;
        if (!readBytes(fd,enc, 4)) return NULL;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    } else {
        Log(LOG_ERROR, "Unknown integer encoding (0x%02x)", enctype);
        return NULL;
    }

    /* convert val into string */
    char *buf;
    buf = malloc(sizeof(char) * 128);
    int n = sprintf(buf, "%lld", val);
    buf[n] = '\0';
    return buf;
}

double* loadDoubleValue(int fd) {
	double R_Zero = 0.0;
	double R_PosInf = 1.0/R_Zero;
	double R_NegInf = -1.0/R_Zero;
	double R_Nan = R_Zero/R_Zero;
    
    char buf[256];
    unsigned char len;
    double* val;

    if (!readBytes(fd,&len,1)) return NULL;

    val = malloc(sizeof(double));

    switch(len) {
    case 255: *val = R_NegInf;  return val;
    case 254: *val = R_PosInf;  return val;
    case 253: *val = R_Nan;     return val;
    default:
        if (!readBytes(fd,buf, len)) {
            free(val);
            return NULL;
        }
        buf[len] = '\0';
        sscanf(buf, "%lg", val);
        return val;
    }
}


int processDoubleValue(int fd, double* store) {
    double *val = loadDoubleValue(fd);
    if (val == NULL) {
        Log(LOG_ERROR, "Error reading double value");
        return 0;
    }

    if (store != NULL) {
        *store = *val;
        free(val);
    } else {
        free(val);
    }
    return 1;
}

char* loadStringObject(int fd) {
    int isencoded;
    uint32_t len;

    len = loadLength(fd, &isencoded);
    //printf("%d\n",len );
    //Log(LOG_NOTICE,"length %d",len);
    if (isencoded) {
        switch(len) {
        case REDIS_RDB_ENC_INT8:
        case REDIS_RDB_ENC_INT16:
        case REDIS_RDB_ENC_INT32:
            return loadIntegerObject(fd,len);
        case REDIS_RDB_ENC_LZF:
            return loadLzfStringObject(fd);
        default:
            /* unknown encoding */
            Log(LOG_ERROR, "Unknown string encoding (0x%02x)", len);
            return NULL;
        }
    }

    if (len == REDIS_RDB_LENERR) return NULL;
    Log(LOG_DEBUG,"malloc length %d",len);
    char *buf = malloc(sizeof(char) * (len+1));
    if (buf == NULL) return NULL;
    buf[len] = '\0';
    //处理空字符串的情况
    if(len == 0){
        return buf;
    }
    if (!readBytes(fd,buf, len)) {
        free(buf);
        return NULL;
    }
    return buf;
}

int processStringObject(int fd, char** store) {
    char *key = loadStringObject(fd);
    if (key == NULL) {
        Log(LOG_ERROR, "Error reading string object");
        //free(key);
        return 0;
    }

    if (store != NULL) {
        *store = key;
    } else {
        free(key);
    }
    return 1;
}

int loadPair(thread_contex * th) {
    uint32_t i,k;
    uint32_t length = 0;

    /* read key first */
    char *key;
    struct rlistset * lnode;
    struct rzset * znode;
    struct rhash * hnode;

    unsigned char *zl;
    unsigned char *zi;
    unsigned char *fstr, *vstr;
    unsigned int flen, vlen;
    long long sval;
    uint64_t isvalue;
    char * buf;
    intset *is;

    if (processStringObject(th->fd, &key)) {
        th->key = key;
    } else {
        Log(LOG_ERROR,"Error reading entry key, server %s:%d",th->sc->pname,th->sc->port);
        return 0;
    }

    Log(LOG_DEBUG, "the key is %s",th->key);

    if (th->type == REDIS_LIST ||
        th->type == REDIS_SET  ||
        th->type == REDIS_ZSET ||
        th->type == REDIS_HASH) {
        if ((length = loadLength(th->fd,NULL)) == REDIS_RDB_LENERR) {
            Log(LOG_ERROR ,"Error reading %d length, server %s:%d", th->type,th->sc->pname,th->sc->port);
            return 0;

        }
    }

    switch(th->type) {
    case REDIS_HASH_ZIPMAP:
    	if (!processStringObject(th->fd, (char **)&zl)) {
            Log(LOG_ERROR, "Error reading entry value, type is %d ,server %s:%d",th->type,th->sc->pname,th->sc->port);
            return 0;
        }
        th->value->hash = newHash();
        zi = zipmapRewind(zl);
        while((zi = zipmapNext(zi, &fstr, &flen, &vstr, &vlen)) != NULL){
        		hnode = hashAdd(th->value->hash);
        		hnode->field = malloc(flen+1);
        		memcpy(hnode->field , fstr,flen);
        		hnode->field[flen] = '\0';
        		hnode->value = malloc(vlen+1);
        		memcpy(hnode->value , vstr,vlen);
        		hnode->value[vlen] = '\0';
        		Log(LOG_DEBUG,"REDIS_HASH_ZIPMAP key: %s , field %s , value %s , server %s:%d", th->key, hnode->field, hnode->value,th->sc->pname,th->sc->port);
        	}
        	free(zl);
        	return 1;
        	break;

    case REDIS_LIST_ZIPLIST:
    	if (!processStringObject(th->fd, (char **)&zl)) {
            Log(LOG_ERROR, "Error reading entry value , type %d, server %s:%d",th->type,th->sc->pname,th->sc->port);
            return 0;
        }
        th->value->listset = newListSet();
        zi = ziplistIndex(zl,0);
        while(zi){
        	lnode = listSetAdd(th->value->listset);
        	ziplistGet(zi, &vstr, &vlen, &sval);
        	
        	if (vstr) {
        		lnode->str = malloc(vlen+1);
        		memcpy(lnode->str,vstr,vlen);
        		lnode->str[vlen] = '\0';
            } else {
            	buf = malloc(256);
            	sdsll2str(buf,sval);
            	lnode->str = buf;
            }
            Log(LOG_DEBUG, "REDIS_LIST_ZIPLIST key: %s ,value %s , server %s:%d",th->key, lnode->str,th->sc->pname,th->sc->port);
        	zi=ziplistNext(zl,zi);
        }
        free(zl);
        return 1;
        break;

    case REDIS_SET_INTSET:
    	 if (!processStringObject(th->fd, (char **)&is)) {
            Log(LOG_ERROR, "Error reading entry value , type %d, server %s:%d",th->type,th->sc->pname,th->sc->port);
            return 0;
        }
        th->value->listset = newListSet();
        length = intsetLen(is);
        for (i = 0; i < length; ++i){
        	lnode = listSetAdd(th->value->listset);
     		intsetGet(is, i, &isvalue);
     		printf("%ld\n",isvalue );
     		buf = malloc(256);
            sdsll2str(buf,isvalue);
            lnode->str = buf;
            Log(LOG_NOTICE, "REDIS_SET_INTSET key : %s ,value %s , server %s:%d",th->key, lnode->str,th->sc->pname,th->sc->port);
        }
        free(is);
        return 1;
        break;

    case REDIS_ZSET_ZIPLIST:
    	if (!processStringObject(th->fd, (char **)&zl)) {
            Log(LOG_ERROR, "Error reading entry value , type %d, server %s:%d",th->type,th->sc->pname,th->sc->port);
            return 0;
        }
    	th->value->zset = newZset();
    	zi = ziplistIndex(zl,0);
        while(zi){
        	znode = zsetAdd(th->value->zset);
        	//value
        	ziplistGet(zi, &vstr, &vlen, &sval);
        	
        	if (vstr) {
        		znode->str = malloc(vlen+1);
        		memcpy(znode->str,vstr,vlen);
        		znode->str[vlen] = '\0';
            } else {
            	buf = malloc(256);
            	sdsll2str(buf,sval);
            	znode->str = buf;
            }
            Log(LOG_DEBUG, "REDIS_ZSET_ZIPLIST key : %s , value %s , server %s:%d",th->key, znode->str,th->sc->pname,th->sc->port);

            //score
        	zi=ziplistNext(zl,zi);
        	znode->score = zzlGetScore(zi);
        	Log(LOG_DEBUG, "REDIS_ZSET_ZIPLIST key : %s , score %f , server %s:%d",th->key, znode->score,th->sc->pname,th->sc->port);
        	zi=ziplistNext(zl,zi);
        }
        free(zl);
        return 1;
        break;
    case REDIS_HASH_ZIPLIST:
    	if (!processStringObject(th->fd, (char **)&zl)) {
            Log(LOG_ERROR, "Error reading entry value , type %d, server %s:%d",th->type,th->sc->pname,th->sc->port);
            return 0;
        }
    	th->value->hash = newHash();
    	zi = ziplistIndex(zl,0);
    	while(zi){
        	hnode = hashAdd(th->value->hash);
        	//field
        	ziplistGet(zi, &vstr, &vlen, &sval);
        	
        	if (vstr) {
        		hnode->field = malloc(vlen+1);
        		memcpy(hnode->field,vstr,vlen);
        		hnode->field[vlen] = '\0';
            } else {
            	buf = malloc(256);
            	sdsll2str(buf,sval);
            	hnode->field = buf;
            }
            Log(LOG_DEBUG, "REDIS_HASH_ZIPLIST  key : %s , field %s , server %s:%d",th->key,hnode->field, th->sc->pname,th->sc->port);
            zi=ziplistNext(zl,zi);
            //value
            ziplistGet(zi, &vstr, &vlen, &sval);
        	
        	if (vstr) {
        		hnode->value = malloc(vlen+1);
        		memcpy(hnode->value,vstr,vlen);
        		hnode->value[vlen] = '\0';
            } else {
            	buf = malloc(256);
            	sdsll2str(buf,sval);
            	hnode->value = buf;
            }
            Log(LOG_DEBUG, "REDIS_HASH_ZIPLIST  key :%s , value %s , server %s:%d",th->key,hnode->value,th->sc->pname,th->sc->port);
            zi=ziplistNext(zl,zi);
        }
        free(zl);
    	return 1;
    	break;
    case REDIS_STRING:
    if (!processStringObject(th->fd,&key)) {
            Log(LOG_ERROR, "Error reading entry value , type %d, server %s:%d",th->type,th->sc->pname,th->sc->port);
            return 0;
        }
        th->value->str = key;
        Log(LOG_DEBUG, "REDIS_STRING  key : %s ,value is %s,valuelength %d , server %s:%d" ,th->key , th->value->str,strlen(th->value->str), th->sc->pname,th->sc->port);
        
        return 1;
    break;
        
    case REDIS_LIST:
    case REDIS_SET:
    th->value->listset = newListSet();
    for (i = 0; i < length; i++) {
    	lnode = listSetAdd(th->value->listset); 
        if (!processStringObject(th->fd,&lnode->str)) {
                Log(LOG_ERROR, "Error reading element at index %d (length: %d), server %s:%d", i, length, th->sc->pname,th->sc->port);
                return 0;
            }
            Log(LOG_DEBUG, "REDIS_LIST/SET key is %s , value %s , server %s:%d",th->key, lnode->str,th->sc->pname,th->sc->port);
            //Log("[notice] listset node value is %s",lnode->str);
        }
        return 1;
    break;

    case REDIS_ZSET:
    	th->value->zset = newZset();
        for (i = 0; i < length; i++) {
        	znode = zsetAdd(th->value->zset);
            if (!processStringObject(th->fd,&znode->str)) {
                Log(LOG_ERROR, "Error reading element at index %d (length: %d), server %s:%d", i, length, th->sc->pname,th->sc->port);
                return 0;
            }
            Log(LOG_DEBUG,"REDIS_ZSET key : %s , value %s , server %s:%d",th->key, znode->str, th->sc->pname,th->sc->port);

            if (!processDoubleValue(th->fd,&znode->score)) {
                Log(LOG_ERROR, "Error reading element at index %d (length: %d), server %s:%d", i, length, th->sc->pname,th->sc->port);
                return 0;
            }
            Log(LOG_DEBUG, "REDIS_ZSET_ZIPLIST key : %s , score %f ,server %s:%d",th->key, znode->score,th->sc->pname,th->sc->port);
        }
        return 1;
    break;

    case REDIS_HASH:
    	th->value->hash = newHash();
        for (i = 0; i < length; i++) {
        	hnode = hashAdd(th->value->hash);
            if (!processStringObject(th->fd,&hnode->field)) {
                Log(LOG_ERROR, "Error reading element at index %d (length: %d), server %s:%d", i, length, th->sc->pname,th->sc->port);
                return 0;
            }
            Log(LOG_DEBUG, "REDIS_HASH  key : %s , field %s ,server %s:%d",th->key,hnode->field,th->sc->pname,th->sc->port);

            if (!processStringObject(th->fd,&hnode->value)) {
                Log(LOG_ERROR, "Error reading element at index %d (length: %d), server %s:%d", i, length, th->sc->pname,th->sc->port);
                return 0;
            }
            Log(LOG_DEBUG, "REDIS_HASH  key :%s , value %s ,server %s:%d",th->key,hnode->value,th->sc->pname,th->sc->port);
        }
        return 1;
    break;

    default:
        Log(LOG_ERROR,"Type not implemented ,type %d, server %s:%d",th->type,th->sc->pname,th->sc->port);
        return 0;
    }
}

void formatResponse(thread_contex *th, buf_t * out){
	int cmd_length;
	int line = 0;
	struct rlistset * listset;
	struct rzset * zset;
	struct rhash * hash;
    //del
    out->position += sprintf(out->position,"*2\r\n$3\r\ndel\r\n");
    out->position+=formatStr(out->position,th->key);
	switch(th->type){
		case REDIS_STRING:
			//*3\r\n
			//memcpy(out->position, "*3\r\n$3\r\nset\r\n",12);
			//out->position += 12;
			out->position += sprintf(out->position,"*3\r\n$3\r\nset\r\n");
			out->position+=formatStr(out->position,th->key);
			out->position+=formatStr(out->position,th->value->str);
				break;
		case REDIS_LIST:
		case REDIS_LIST_ZIPLIST:
			listset = th->value->listset;
			listset = listset->next;
			//$5\r\nrpush\r\n
			out->position += sprintf(out->position,"*%d\r\n$5\r\nrpush\r\n",th->bucknum+2);
			out->position+=formatStr(out->position,th->key);
			while(listset){
				out->position+=formatStr(out->position,listset->str);
				listset = listset->next;
			}
			break;
		case REDIS_SET:
		case REDIS_SET_INTSET:
			listset = th->value->listset;
			listset = listset->next;
			out->position += sprintf(out->position,"*%d\r\n$4\r\nsadd\r\n",th->bucknum+2);
			out->position+=formatStr(out->position,th->key);
			while(listset){
				line++;
				out->position+=formatStr(out->position,listset->str);
				listset = listset->next;
			}
			break;
		case REDIS_ZSET:
		case REDIS_ZSET_ZIPLIST:
			zset = th->value->zset;
			zset = zset->next;
			out->position += sprintf(out->position,"*d\r\n$4\r\nzadd\r\n",th->bucknum+2);
			out->position+=formatStr(out->position,th->key);
			//$4\r\nzadd\r\n;
			while(zset){
				out->position += formatDouble(out->position,zset->score);
				out->position+=formatStr(out->position,zset->str);
				zset = zset->next;
			}
			break;
		case REDIS_HASH:
		case REDIS_HASH_ZIPMAP:
		case REDIS_HASH_ZIPLIST:
			hash = th->value->hash;
			hash = hash->next;
			//$5\r\nhmset\r\n;
			out->position += sprintf(out->position,"*%d\r\n$5\r\nhmset\r\n",th->bucknum+2);
			out->position+=formatStr(out->position,th->key);
			while(hash){
				//printf("%s\n",hash->field);
				out->position+=formatStr(out->position,hash->field);
				out->position+=formatStr(out->position,hash->value);
				hash = hash->next;
			}
			break;
	}

    //ttl
    if(th->expiretime != -1 || th->expiretimeM != -1){
        //*3\r\n$8\r\nexpireat\r\n$n\r\nkey\r\n
        out->position += sprintf(out->position,"*3\r\n$9\r\npexpireat\r\n");
        out->position+=formatStr(out->position,th->key);
        out->position += sprintf(out->position,"$%lld\r\n%lld\r\n",lengthSize(th->expiretimeM),th->expiretimeM);
    }
}

int responseSize(thread_contex *th){
	int cmd_length;
	int line = 0;
	struct rlistset * listset;
	struct rzset * zset;
	struct rhash * hash;
    //delete first
    // *2\r\n$3del\r\n
    cmd_length = 11;
    cmd_length += lengthSize(strlen(th->key))+5+strlen(th->key);

    //expire
    if(th->expiretime != -1 || th->expiretimeM != -1){
        if(th->expiretime != -1){
            th->expiretimeM = th->expiretime * 1000;
        }
        //*3\r\n$8\r\npexpireat\r\n$n\r\nkey\r\n
        cmd_length += 19;
        cmd_length += lengthSize(strlen(th->key))+5+strlen(th->key);
        cmd_length += lengthSize(lengthSize(th->expiretimeM))+5+lengthSize(th->expiretimeM);
    }
	switch(th->type){
		case REDIS_STRING:
			//*3\r\n
			cmd_length += 4;
			//$3\r\nset\r\n
			cmd_length += 9;
			//$keylength\r\nkey\r\n
			cmd_length += lengthSize(strlen(th->key))+5+strlen(th->key);
			//$valuelength\r\nvalue\r\n
			cmd_length += lengthSize(strlen(th->value->str))+5+strlen(th->value->str);
				break;
		case REDIS_LIST:
		case REDIS_LIST_ZIPLIST:
			listset = th->value->listset;
			listset = listset->next;
			//$5\r\nrpush\r\n
			cmd_length += 11;
			cmd_length += lengthSize(strlen(th->key))+5+strlen(th->key);
			while(listset){
				//printf("%s\n", listset->str);
				line++;
				cmd_length += lengthSize(strlen(listset->str))+5+strlen(listset->str);
				listset = listset->next;
			}
			th->bucknum = line;
			cmd_length += lengthSize(line+2)+3;
			break;
		case REDIS_SET:
		case REDIS_SET_INTSET:
			listset = th->value->listset;
			listset = listset->next;
			//$4\r\nsadd\r\n
			cmd_length += 10;
			cmd_length += lengthSize(strlen(th->key))+5+strlen(th->key);
			while(listset){
				line++;
				cmd_length += lengthSize(strlen(listset->str))+5+strlen(listset->str);
				listset = listset->next;
			}
			th->bucknum = line;
			cmd_length += lengthSize(line+2)+3;
			break;
		case REDIS_ZSET:
		case REDIS_ZSET_ZIPLIST:
			zset = th->value->zset;
			zset = zset->next;
			//$4\r\nzadd\r\n;
			cmd_length += 10;
			cmd_length += lengthSize(strlen(th->key))+5+strlen(th->key);
			while(zset){
				line++;
				cmd_length += lengthSize(strlen(zset->str))+5+strlen(zset->str);
				line++;
				cmd_length += lengthSize(doubleSize(zset->score))+5+doubleSize(zset->score);
				zset = zset->next;
			}
			th->bucknum = line;
			cmd_length += lengthSize(line+2)+3;
			break;
		case REDIS_HASH:
		case REDIS_HASH_ZIPMAP:
		case REDIS_HASH_ZIPLIST:
			hash = th->value->hash;
			hash = hash->next;
			//$5\r\nhmset\r\n;
			cmd_length += 11;
			cmd_length += lengthSize(strlen(th->key))+5+strlen(th->key);
			while(hash){
				line++;
				cmd_length += lengthSize(strlen(hash->field))+5+strlen(hash->field);
				line++;
				cmd_length += lengthSize(strlen(hash->value))+5+strlen(hash->value);
				hash = hash->next;
			}
			th->bucknum = line;
			cmd_length += lengthSize(line+2)+3;
			break;
	}

	return cmd_length;
}

void appendToOutBuf(thread_contex *th, buf_t * b){
	pthread_mutex_lock(&th->mutex);
	if(!th->bufout){
		//printf("aaa\n");
		th->bufout = th->bufoutLast = b;
	}else{
		//printf("bbb\n");
		th->bufoutLast->next = b;
		th->bufoutLast = b;
	}
	addEvent(th->loop, th->write,EVENT_WRITE);
	pthread_mutex_unlock(&th->mutex);
}
void freeMem(thread_contex * th){
	//free memory
		free(th->key);
		switch(th->type){
			case REDIS_STRING:
                //Log(LOG_NOTICE,"%x",th->value->str);
                //Log(LOG_NOTICE,"%s",th->value->str);
				free(th->value->str);
				break;
			case REDIS_LIST:
			case REDIS_SET:
			case REDIS_LIST_ZIPLIST:
			case REDIS_SET_INTSET:
				freeListSet(th->value->listset);
				break;
			case REDIS_ZSET:
			case REDIS_ZSET_ZIPLIST:
				freeZset(th->value->zset);
				break;
			case REDIS_HASH:
			case REDIS_HASH_ZIPMAP:
			case REDIS_HASH_ZIPLIST:
				freeHash(th->value->hash);
				break;
			}
				return;
}
void processPair(thread_contex *th){
	 
	uint32_t hash = server_hash(server.new_config, th->key, strlen(th->key));
	int index = dispatch(server.new_config,hash);
	server_conf * from = th->sc;
	server_conf * to = array_get(server.new_config->servers,index);
	Log(LOG_DEBUG ,"the key %s from %s:%d",th->key,from->pname,from->port);
	Log(LOG_DEBUG, "the key %s should goto %s:%d",th->key, to->pname, to->port);

	if(strcmp(from->pname,to->pname)==0 && from->port == to->port){
		//printf("the key from is same to %s\n",th->key);
		Log(LOG_DEBUG,"the key %s server is same",th->key);
        freeMem(th);
		return ;
	}

	//send to new redis
    
	int size = responseSize(th);
	buf_t *output = getBuf(size+20);
	if(!output){
        Log(LOG_ERROR,"getBuf error , server %s:%d",th->sc->pname,th->sc->port);
        exit(1);
		//printf("getBuf error\n");
	}
	formatResponse(th, output);
	appendToOutBuf(to->contex, output);
    //freeBuf(output);
	freeMem(th);
	//printf("%s\n",output->start);
	th->processed++;
	return;
}

int parseRdb(thread_contex * th){
	int fd = th->fd;
	int type;
	int n;
	char buf[1024];
    int32_t expiretime;
    int64_t expiretimeM;
    long nread = 0;
    th->value = malloc(sizeof(rvalue));
    th->processed = 0;
    if(!th->value){
    	Log(LOG_ERROR, "malloc error");
    	return 0;
    }
    while(nread < th->transfer_size){
    	th->expiretime = th->expiretimeM = -1;

    	//parse type
    	th->type = loadType(fd);
    	if(th->type == -1){
            Log(LOG_ERROR,"loadType error");
    		return 0;
    	}
    	nread++;
        // Log(LOG_NOTICE,"type is %d",th->type);
    	//printf("type is %d\n",type );
    	if (th->type == REDIS_SELECTDB) {
    		//printf("here \n");
    		loadLength(fd,NULL);
    		th->type = loadType(fd);
    		//do nothing
        }

        if (th->type == REDIS_EOF) {
        	//if (nread < th->transfer_size){
        		//Log("Unexpected EOF");
        		Log(LOG_NOTICE, "server %s:%d, processed %ld keys",th->sc->pname,th->sc->port, th->processed);
        		//skip 8 byte checksum
        		readBytes(fd,buf,8);
        		return 1;
        }else{
        	if (th->type == REDIS_EXPIRETIME ||th->type == REDIS_EXPIRETIME_MS) {
        		//Log(LOG_NOTICE,"the type %d",th->type);

                if (n = processTime(fd,th->type,&th->expiretime,&th->expiretimeM) ==0) {
                    Log(LOG_ERROR,"processTime error");
                    return 0;
                };
                //Log(LOG_NOTICE,"time is %ld",expiretimeM);
        		nread+=n;
        		if ((th->type = loadType(fd) )== -1){
                    //Log(LOG_NOTICE,"type 2 %d",th->type);
                    Log(LOG_ERROR,"loadType error");
                    return 0;
                } 
        		nread++;
        	}
            //Log(LOG_NOTICE,"the type is %d",th->type);
        	//printf("type is %d\n",type );
        	if (n = loadPair(th) ==0) {
        		Log(LOG_ERROR, "server %s:%d parse error",th->sc->pname,th->sc->port);
        		return 0;
        	}

        	processPair(th);
        	nread +=n;
        }
    }
  }
