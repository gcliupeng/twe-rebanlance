#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include "main.h"
int nonBlock(int fd) {
    int flags;

    if ((flags = fcntl(fd, F_GETFL)) == -1) {
        Log(LOG_WARNING ,"fcntl error");
        return -1;
    }

    flags |= O_NONBLOCK;

    if (fcntl(fd, F_SETFL, flags) == -1) {
        Log(LOG_WARNING ,"fcntl error");
        return -1;
    }
    return 0;
}

int connetToServer(int port, char * ip){

	int fd = socket(AF_INET, SOCK_STREAM, 0);

    if(fd<0){
    	Log(LOG_ERROR, "socket error, errno %d",errno);
    	return 0;
    }
    //nonBlock(fd);

    struct sockaddr_in serveraddr;

    bzero(&serveraddr, sizeof(serveraddr));

    serveraddr.sin_family = AF_INET;
    inet_aton(ip,&(serveraddr.sin_addr));
    serveraddr.sin_port=htons(port);

    if(connect(fd, (struct sockaddr*)&serveraddr, sizeof(serveraddr)) <0){
    	Log(LOG_ERROR, "connect error ,ip: %s , port: %d, errno: %d",ip,port,errno);
        return 0;
    }
    return fd;
}

int sendToServer(int fd,char *data, int length){
	int n =0,m;
    while(n<length){
        m = write(fd,data+n,length-n);
        if(m <= 0){
            return 0;
        }
        n+=m;
    }
	return n;
}

int readFromServer(int fd, char * data ,int length){
	int n = read(fd, data,length);
	if(n == 0){
		Log(LOG_ERROR, "read return 0");
		return 0;
	}
	if(n<0){

	}
	return n; 
}

int readLineFromServer(int fd, char * ptr, int max){
    int nread = 0;
    while(max) {
        char c;

        if (read(fd,&c,1) == -1) return -1;
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