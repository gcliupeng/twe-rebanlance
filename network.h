#ifndef NETWORK_H
#define NETWORK_H

int connetToServer(int port, char * ip);
int sendToServer(int fd,char *data, int length);
int readFromServer(int fd, char * data ,int length);
int readLineFromServer(int fd, char * ptr, int max);
int nonBlock(int fd);
#endif