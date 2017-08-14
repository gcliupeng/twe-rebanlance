#ifndef LIST_H
#define LIST_H

#include <stdio.h>
#include <stdlib.h>
#include "ev.h"
#include "main.h"

struct node{
	void * data;
	int timeout;
	struct node * next;
};

struct list{
	struct node * head;
};

#ifndef LIST_T 
typedef struct list list;
#define LIST_T
#endif

static list* newList(){
	list* l =malloc(sizeof(*l));
	l->head = NULL;
}

static int listLength(list *l){
	int length = 0;
	struct node * h = l->head;
	while(h){
		length++;
		h = h->next;
	}
	return length;
}
static int listInsert(list *l,long timeout,void *data){
	if(listLength(l) == 0){
		struct node * n = malloc(sizeof(*n));
		if(!n){
			return -1;
		}
		n->data = data;
		n->timeout = timeout;
		l->head = n;
		return 1;
	}
	struct node * h = l->head;
	while(h->next && h->next->timeout < timeout) h = h->next;
	struct node *n = malloc(sizeof(*n));
	if(!n){
		return -1;
	}
	n->data = data;
	n->timeout = timeout;
	h->next = n;
	return 1;
}
void expireTimeout(list *l,long now){
	if(listLength(l) == 0){
		return;
	}
	struct node * h = l->head;
	struct node * t;
	//callBack tcall;
	event * e;
	while(h){
		if(h->timeout < now){
			Log(LOG_DEBUG ,"timeout expired");
			e = h->data;
			//tcall = e->tcall;
			e->tcall(e);
			l->head = h->next;
			t = h;
			h=h->next;
			free(t);
		}else{
			return;
		}
	}
}
#endif