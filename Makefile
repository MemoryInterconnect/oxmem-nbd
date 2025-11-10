.PHONY: default
default: all ;

# Build only oxmem-nbd by default
all: oxmem-nbd

oxmem-nbd: oxmem-nbd.c lib-ox-packet.c lib-queue.c Makefile lib-ox-packet.h lib-queue.h
	gcc -D_FILE_OFFSET_BITS=64 -o $@ oxmem-nbd.c lib-ox-packet.c lib-queue.c -lpthread
	sudo setcap cap_net_raw+ep $@

make-increment-file: make-increment-file.c Makefile
	gcc make-increment-file.c -o $@

test-nbd-devices: test-nbd-devices.c Makefile
	gcc -D_FILE_OFFSET_BITS=64 -o $@ test-nbd-devices.c

.PHONY: clean

clean:
	rm -f oxmem-nbd make-increment-file test-nbd-devices
