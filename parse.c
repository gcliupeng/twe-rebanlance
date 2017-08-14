#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "main.h"
#include "network.h"
#include "parse.h"


char *rdbLoadLzfStringObject(thread_contex *th) {
    unsigned int len, clen;
    unsigned char *c = NULL;
    sds val = NULL;

    if ((clen = rdbLoadLen(th)) == -1) return NULL;
    if ((len = rdbLoadLen(th)) == -1) return NULL;
    if ((c = malloc(clen)) == NULL) goto NULL;
    if ((val = sdsnewlen(NULL,len)) == NULL) goto err;
    if (read(rdb,c,clen) == 0) goto err;
    if (lzf_decompress(c,clen,val,len) == 0) goto err;
    zfree(c);
    return createObject(REDIS_STRING,val);
err:
    zfree(c);
    sdsfree(val);
    return NULL;
}


int ll2str(char *s, long long value) {
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

char * rdbLoadIntegerObject(int fd, int enctype) {
    unsigned char enc[4];
    long long val;

    if (enctype == REDIS_RDB_ENC_INT8) {
        if(read(fd,enc,1) <1) return -1;
        	val = (signed char)enc[0];
    } else if (enctype == REDIS_RDB_ENC_INT16) {
        uint16_t v;
        if (read(fd,enc,2) < 2) return -1;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
    } else if (enctype == REDIS_RDB_ENC_INT32) {
        uint32_t v;
        if (read(rdb,enc,4) < 4) return NULL;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    } else {
        val = 0; /* anti-warning */
        //redisPanic("Unknown RDB integer encoding type");
    }

    char * p = malloc(21);
    ll2str(p,val);
    return p;
}

int rdbGetString(thread_contex *th){
	if(rdbLoadLen(th) ==-1){
		return -1; 
	}
	if(th->encoded){
		switch(th->key_length) {
        	case REDIS_RDB_ENC_INT8:
        	case REDIS_RDB_ENC_INT16:
        	case REDIS_RDB_ENC_INT32:
        		th->tmpStr = rdbLoadIntegerObject(th,key_length);
            	return 1;
        	case REDIS_RDB_ENC_LZF:
            	return rdbLoadLzfStringObject(th);
        	default:
            	//redisPanic("Unknown RDB encoding type");
		}
	}
}

uint32_t rdbLoadLen(thread_contex* th){
	unsigned char buf[2];
    uint32_t len;
    int type;
    int fd = th->fd;
    th->encoded = 0;

    int n = read(fd,buf,1);
    if(n == 0){
    	th->close =1;
    	return -1;
    }

    type = (buf[0]&0xC0)>>6;
    if (type == REDIS_RDB_ENCVAL) {
        /* Read a 6 bit encoding type. */
        th->encoded = 1;
        th->key_length = buf[0]&0x3F;
        return 1;
    } else if (type == REDIS_RDB_6BITLEN) {
        /* Read a 6 bit len. */
        th->key_length = buf[0]&0x3F;
        return 1;
    } else if (type == REDIS_RDB_14BITLEN) {
        /* Read a 14 bit len. */
        n = read(fd,buf+1,1);
        if(n == 0){
        	th->close =1;
        	return -1;
        }
        th->key_length = ((buf[0]&0x3F)<<8)|buf[1];
        return 1;
    } else {
        /* Read a 32 bit len. */
        n = read(rdb,&len,4);
        if(n == 0){
        	th->close =1;
        	return -1;
        }
        th->key_length =ntohl(len);
        return 1;
    }
}

int rdbLoadType(thread_contex *th){
	int n = read(th->fd,&th->type,1);
    if(n < 0){
    	return -1;
    }
    if(n == 0){
    	th->close = 1;
    	return -1;
    }
}

time_t rdbLoadTime(thread_contex *th) {
    int n = read(th->fd,&th->expiretime,4);
    if(n < 0){
    	return -1;
    }
    if(n == 0){
    	th->close = 1;
    	return -1;
    }
    return 1;
}
long long rdbLoadMillisecondTime(thread_contex *th){
	int n = read(th->fd,&th->expiretimeM,8);
    if(n < 0){
    	return -1;
    }
    if(n == 0){
    	th->close = 1;
    	return -1;
    }
    return 1;
}


void parseRdb(void *data){
	event *ev = data;
	thread_contex * th = ev->contex;
	int fd = ev->fd;
	int type;
	int n;
	char buf[1024];
    long expiretime;
    //buf_t * buf;
    printf("%d\n",th->state );

while(1){
	switch(th->state){
		case NEED_HEAD:
			n = readFromServer(fd,buf,9);
    		if(n < 9){
    			return;
    		}
    		buf[9] = '\0';
    		printf("%s\n",buf);
    		th->state = NEED_TYPE;
    		continue;
    		break;
		case NEED_TYPE:
			// printf("ssss\n");
			if(rdbLoadType(th) == -1){
				if(th->close)
            		goto eoferr;
            	return;
			}
			printf("type is %d\n",th->type);

			if(th->type == REDIS_RDB_OPCODE_EXPIRETIME){
            	if(rdbLoadTime(th) == -1){
            		if(th->close)
            			goto eoferr;
            		th->state = NEED_EXPIRE;
            		return;
            	}
            	th->state = NEED_TYPE;
        	}else if(th->type == REDIS_RDB_OPCODE_EXPIRETIME_MS){
            	if(rdbLoadMillisecondTime(th) == -1){
            		if(th->close)
            			goto eoferr;
            		th->state = NEED_EXPIREM;
            		return;
            	}
            	th->state = NEED_TYPE;
        	}else if(th->type == REDIS_RDB_OPCODE_EOF){
        		//th->state = NEED_TYPE;
        	} /* can't appear */
        	else if(th->type == REDIS_RDB_OPCODE_SELECTDB) {
        		//printf("come h\n");
        		th->state = NEED_TYPE;
        	}else{
        		//printf("sdfsfs\n");
				th->state = NEED_KEY;
        	}
			break;
		case NEED_KEY_LENGTH:
			if(rdbLoadLen(th) == -1){

			}
			if(th->encoded){
				//key已获取
				if(th->key_length == REDIS_RDB_ENC_INT8 || th->key_length == REDIS_RDB_ENC_INT16|| th->key_length == REDIS_RDB_ENC_INT32){
	        		th->value = rdbLoadIntegerObject(th,key_length);
	        		th->state = NEED_FLUSH;
	        		continue;
				}else{
					
				}
			 
			 }

			//printf("sdfsadf\n");
			//readLineFromServer(fd,buf,1024);
	}

}
eoferr:
	return;
	
}