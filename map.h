#ifndef MAP_H
#define MAP_H
uint hash(char * s,int length){
	uint ini = 0;
	while(length--){
		ini = ini<<5 + *s++;
	}
	return ini;
}

int compare(char *p1,int length1,char *p2,int length2){
	if(length2==length1)
		return strncmp(p1,p2,length1);
	else
		return length1>length2;
}

typedef struct entry
{
	char * key;
	int key_length;
	void * value;
	struct entry * next;
}entry;

typedef struct dict{
	uint size;
	uint mask;
	entry **ht;
}dict;

dict* createDict(int size){
	dict *d = malloc(sizeof(*d));
	d->mask = size-1;
	d->size = 0;
	d->ht = malloc(sizeof(entry *)*size);
	int i;
	for (i = 0; i < size; ++i)
	{
		d->ht[i] = NULL;
	}
	return d;

}

entry * dictAdd(dict * d,char * key ,int key_length,void * value){
	uint h = hash(key,key_length);
	uint hash = h & (d->mask);
	entry *p=d->ht[hash];
	while(p){
		if(!compare(key,key_length,p->key,p->key_length)){
			// p->value = value;
			// return p;
			return NULL;
		}
		p = p->next;
	}
	entry *np = malloc(sizeof(entry));
	np->key = key;
	np->key_length = key_length;
	np->value = value;
	np->next = d->ht[hash];
	d->ht[hash] = np;
	d->size++;
	return np;
}
entry * dictFind(dict *d ,char *key,int key_length){
	uint h = hash(key,key_length);
	uint index =h & (d->mask);
	entry *p = d->ht[index];
	while(p){
		if(!compare(key,key_length, p->key,p->key_length)){
			return p;
		}
		p=p->next;
	}
	return NULL;
}
#endif