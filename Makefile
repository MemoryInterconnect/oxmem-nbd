.PHONY: default
default: all ;

# Build only oxmem-nbd by default
all: oxmem-nbd make-increment-file diff_and_show

oxmem-nbd: oxmem-nbd.c lib-ox-packet.c lib-queue.c Makefile lib-ox-packet.h lib-queue.h
	gcc -g -o $@ oxmem-nbd.c lib-ox-packet.c lib-queue.c -pthread
	sudo setcap cap_net_raw+ep $@

make-increment-file: make-increment-file.c Makefile
	gcc make-increment-file.c -o $@

diff_and_show: diff_and_show.c Makefile
	gcc -o $@ diff_and_show.c

test-nbd-devices: test-nbd-devices.c Makefile
	gcc -D_FILE_OFFSET_BITS=64 -o $@ test-nbd-devices.c

.PHONY: clean

clean:
	rm -f oxmem-nbd make-increment-file test-nbd-devices diff_and_show
