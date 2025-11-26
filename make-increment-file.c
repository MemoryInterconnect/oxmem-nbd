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
    unsigned int *buffer;
    size_t size;
    size_t i;
    ssize_t nwrite;
    size_t total_written = 0;

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

    buffer = (unsigned int *) malloc(size);
    if (!buffer) {
	printf("malloc failed for size %zu\n", size);
	close(fd);
	return 1;
    }

    for (i = 0; i < size / sizeof(unsigned int); i++) {
	buffer[i] = (unsigned int)i;
    }

    while (total_written < size) {
	nwrite = write(fd, (char *)buffer + total_written, size - total_written);
	if (nwrite < 0) {
	    printf("write error, errno=%d\n", errno);
	    break;
	}
	total_written += nwrite;
    }

    printf("written %zu bytes\n", total_written);
    free(buffer);
    close(fd);

    return 0;
}
