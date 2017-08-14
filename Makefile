
CC= gcc -std=gnu99
CFLAGS= -g
MYLDFLAGS= -lm -lpthread


TARGET = twe-rebanlance
OBJS = main.o array.o config.o dist.o md5.o struct.o network.o loop.o ev.o parse2.o lzf_c.o lzf_d.o endianconv.o zipmap.o ziplist.o intset.o hash.o buf.o

all:$(TARGET)

$(TARGET):$(OBJS)
	$(CC) -o $@ $(CFLAGS) $(OBJS) $(MYLDFLAGS)

clean:
	rm -rf *.o
	rm -rf twe-rebanlance

array.o: array.c array.h
config.o: config.c config.h
main.o: config.c main.c array.c  
dist.o: dist.c
md5.o: md5.c
struct.o : struct.c
network.o:network.c
loop.o:loop.c
ev.o :ev.c
parse2.o :parse2.c
lzf_d.o :lzf_d.c
lzf_c.o: lzf_c.c
endianconv.o : endianconv.c
zipmap.o: zipmap.c
ziplist.o :ziplist.c
intset.o :intset.c
hash.o :hash.c
buf.o :buf.c