#ifndef ZIP_LIST_H
#define ZIP_LIST_H

unsigned char *ziplistIndex(unsigned char *zl, int index) ;
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p);
unsigned int ziplistGet(unsigned char *p, unsigned char **sstr, unsigned int *slen, long long *sval);
double zzlGetScore(unsigned char *sptr);

#endif