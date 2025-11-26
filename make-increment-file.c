#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>

int main(int argc, char **argv)
{
    int fd;
    unsigned int buffer[1024];  /* 4KB buffer (1024 * 4 bytes) */
    size_t size;
    size_t total_written = 0;
    size_t chunk_size;
    size_t i;
    unsigned int counter = 0;
    ssize_t nwrite;

    if (argc < 3) {
    	printf("Usage: %s [file] [size]\n", argv[0]);
	    return 0;
    }

    fd = open(argv[1], O_CREAT | O_RDWR | O_TRUNC, 0666);

    if (fd < 0) {
    	printf("file open error - %s, errno=%d\n", argv[1], errno);
	    return 0;
    }

    size = atoll(argv[2]);
    printf("size = %zu\n", size);

    while (total_written < size) {
	    chunk_size = size - total_written;
	    if (chunk_size > sizeof(buffer))
	        chunk_size = sizeof(buffer);

	    for (i = 0; i < chunk_size / sizeof(unsigned int); i++) {
	        buffer[i] = counter++;
	    }

	    nwrite = write(fd, buffer, chunk_size);
	    if (nwrite < 0) {
	        printf("write error, errno=%d\n", errno);
	        break;
    	}

	    total_written += nwrite;
    }

    printf("written %zu bytes\n", total_written);
    close(fd);

    return 0;
}

