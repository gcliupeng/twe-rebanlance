#include <stdio.h>
#include <stdlib.h>
#include "struct.h"

struct rlistset * newListSet(){
	struct rlistset * r = malloc(sizeof(*r));
	r->next = r->last = NULL;
}

struct rlistset * listSetAdd(struct rlistset * l){
	struct rlistset * r = malloc(sizeof(*r));
	r->next =NULL;
	if(!l->next){
		l->next=l->last = r;
		return r;
	}
	l->last->next = r;
	l->last = r;
	r->next = NULL;
	return r;
}

struct rzset * newZset(){
	struct rzset * r = malloc(sizeof(*r));
	r->next = r->last = NULL;
}

struct rzset * zsetAdd(struct rzset * z){
	struct rzset * r = malloc(sizeof(*r));
	r->next = NULL;
	if(!z->next){
		z->next=z->last = r;
		return r;
	}
	z->last->next = r;
	z->last = r;
	return r;
}

struct rhash * newHash(){
	struct rhash * r = malloc(sizeof(*r));
	r->next = r->last = NULL;
}

struct rhash * hashAdd(struct rhash * h){
	struct rhash * n = malloc(sizeof(*n));
	n->next = NULL;
	if(!h->next){
		h->next=h->last = n;
		return n;
	}
	h->last->next = n;
	h->last = n;
	return n;
}

void freeListSet(struct rlistset * l){
	struct rlistset * n;
	l = l->next;
	while(l){
		n = l->next;
		free(l->str);
		free(l);
		l = n;
	}
}

void freeHash(struct rhash * h){
	struct rhash * n;
	h = h->next;
	while(h){
		n = h->next;
		free(h->field);
		free(h->value);
		free(h);
		h = n;
	}
}

void freeZset(struct rzset * z){
	struct rzset * n;
	z = z->next;
	while(z){
		n = z->next;
		free(z->str);
		free(z);
		z = n;
	}
}
