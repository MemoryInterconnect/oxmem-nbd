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
    int nread, nwrite, i;

    if (argc < 2) {
	printf("Usage: %s [file] [size]\n", argv[0]);
	return 0;
    }

    fd = open(argv[1], O_CREAT | O_RDWR, 0666);

    if (fd < 0) {
	printf("file open error - %s, errno=%d\n", argv[1], errno);
	return 0;
    }

    size = atoll(argv[2]);
    printf("size = %lu\n", size);

    buffer = (unsigned int *) malloc(size);

    for (i = 0; i < size / sizeof(unsigned int); i++) {
	buffer[i] = i;
    }

    nwrite = write(fd, buffer, size);

    close(fd);

    return 0;
}
