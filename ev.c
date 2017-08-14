#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include "ev.h"
#include "list.h"

eventLoop * eventLoopCreate(){
	int efd = epoll_create(500);
	if(efd < 0){
		Log(LOG_ERROR, "epoll_create error");
		return NULL;
	}
	eventLoop * ev =malloc(sizeof(*ev));
	if(!ev){
		Log(LOG_ERROR,"malloc error");
		return NULL;
	}
	list * newl = newList();
	ev->timeoutList = newl;
	if(!ev->timeoutList){
		free(ev);
		Log(LOG_ERROR,"malloc error");
		return NULL;
	}
	ev->efd = efd;
	return ev;
}

int addEvent(eventLoop* loop,event *ev, int type){
	int op;
	if((type == EVENT_READ && ev->ractive) || (type == EVENT_WRITE && ev->wactive)){
		return 1;
	}
	if(ev->ractive||ev->wactive){
		op = EPOLL_CTL_MOD;
	}else{
		op = EPOLL_CTL_ADD;
	}
	//printf("come here\n");
	struct epoll_event e;
	long timeout ;
	switch(type){
		case EVENT_READ:
				e.data.ptr = ev; 
				e.events=EPOLLIN;
				ev->ractive = 1;
				return epoll_ctl(loop->efd, op, ev->fd, &e);
				break;
		case EVENT_WRITE:
				e.data.ptr = ev; 
				e.events=EPOLLOUT;
				ev->wactive = 1;
				return epoll_ctl(loop->efd, op, ev->fd, &e);
				break;
		case EVENT_TIMEOUT:
				timeout = ev->timeout;
				struct timeval   tv;
				gettimeofday(&tv, NULL);
				timeout = timeout+tv.tv_sec*1000+tv.tv_usec/1000;
				return listInsert(loop->timeoutList,timeout,ev);
				break;
	}
	return 0;
}

void delEvent(eventLoop* loop,event *ev, int type){
	int op ;
	event * other;
	thread_contex * th = ev->contex;
	struct epoll_event e;
	if((type == EVENT_READ && !ev->ractive) || (type == EVENT_WRITE && !ev->wactive)){
		return ;
	}
	if(type == EVENT_READ){
		other = th->write;
		if(other->wactive){
			op = EPOLL_CTL_MOD;
			e.events = EPOLLOUT;
			e.data.ptr = other;
		}else{
			op = EPOLL_CTL_DEL;
		}
		ev->ractive = 0;
	}else{
		other = th->read;
		if(other->ractive){
			op = EPOLL_CTL_MOD;
			e.events = EPOLLIN;
			e.data.ptr = other;
		}else{
			op = EPOLL_CTL_DEL;
		}
		ev->wactive = 0;
	}
	epoll_ctl(loop->efd, op, ev->fd, &e);

}

void eventCycle(eventLoop* loop){
	int n,i;
	int fd;
	event * ev;
	long minTimeout ,now;
	while(1){
		if(listLength(loop->timeoutList) == 0){
			minTimeout = -1;
		}else{
			struct timeval   tv;
			gettimeofday(&tv, NULL);
			minTimeout = loop->timeoutList->head->timeout;
			now = tv.tv_sec*1000+tv.tv_usec/1000;
			minTimeout = minTimeout - now;
			if(minTimeout < 0){
				minTimeout = 0;
			} 
		}
		n = epoll_wait(loop->efd,loop->list,500,minTimeout);
		if(n < 0){
			Log(LOG_ERROR, "epoll_wait return %d",n);
			return ;
		}
		//Log("[notice] epoll_wait return %d",n);
		for (i = 0; i < n; ++i)
		{
			struct epoll_event e = loop->list[i];
			ev = (event *)e.data.ptr;

			if(e.events & EPOLLIN){
				//Log("[notice] fd %d , read ready ~",ev->fd);
				ev->rcall(ev);
			}
			if(e.events & EPOLLOUT){
				//Log("[notice] fd %d , write ready ~",ev->fd);
				ev->wcall(ev);
			}
		}
		//timeout
		expireTimeout(loop->timeoutList,now);
	}
}