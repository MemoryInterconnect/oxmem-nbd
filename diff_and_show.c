#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <termios.h>
#include <sys/ioctl.h>

#define KEY_SPACE  ' '
#define KEY_PGDN   1
#define KEY_PGUP   2
#define KEY_QUIT   'q'

static int get_terminal_height(void)
{
    struct winsize ws;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == -1)
        return 24;  /* default fallback */
    return ws.ws_row;
}

static int get_key(void)
{
    struct termios old_term, new_term;
    int ch;
    unsigned char seq[3];

    tcgetattr(STDIN_FILENO, &old_term);
    new_term = old_term;
    new_term.c_lflag &= ~(ICANON | ECHO);
    tcsetattr(STDIN_FILENO, TCSANOW, &new_term);

    ch = getchar();

    if (ch == 27) {  /* escape sequence */
        seq[0] = getchar();
        seq[1] = getchar();
        if (seq[0] == '[') {
            if (seq[1] == '5') {  /* PgUp: ESC [ 5 ~ */
                getchar();  /* consume '~' */
                ch = KEY_PGUP;
            } else if (seq[1] == '6') {  /* PgDn: ESC [ 6 ~ */
                getchar();  /* consume '~' */
                ch = KEY_PGDN;
            }
        }
    }

    tcsetattr(STDIN_FILENO, TCSANOW, &old_term);
    return ch;
}

static void print_row(size_t offset, unsigned char *data1, unsigned char *data2, size_t len)
{
    size_t i;
    uint32_t val;

    /* Print offset */
    printf("0x%012zx: ", offset);

    /* Print file1 as 4-byte units */
    for (i = 0; i < 16; i += 4) {
        if (i + 4 <= len) {
            memcpy(&val, &data1[i], 4);
            printf("%08X ", val);
        } else if (i < len) {
            /* Partial 4-byte unit at end */
            val = 0;
            memcpy(&val, &data1[i], len - i);
            printf("%08X ", val);
        } else {
            printf("         ");
        }
    }

    printf("\n              : ");

    /* Print file2 as 4-byte units */
    for (i = 0; i < 16; i += 4) {
        if (i + 4 <= len) {
            memcpy(&val, &data2[i], 4);
            printf("%08X ", val);
        } else if (i < len) {
            /* Partial 4-byte unit at end */
            val = 0;
            memcpy(&val, &data2[i], len - i);
            printf("%08X ", val);
        } else {
            printf("         ");
        }
    }

    printf("\n");
}

int main(int argc, char **argv)
{
    int fd1, fd2;
    struct stat st1, st2;
    unsigned char *map1, *map2;
    size_t size;
    size_t i;
    size_t diff_row_count = 0;
    size_t *diff_row_offsets = NULL;
    size_t diff_capacity = 1024;
    int term_height;
    size_t page_size;
    size_t current_pos = 0;
    int key;

    if (argc != 3) {
        printf("Usage: %s <file1> <file2>\n", argv[0]);
        return 1;
    }

    fd1 = open(argv[1], O_RDONLY);
    if (fd1 < 0) {
        printf("Cannot open %s\n", argv[1]);
        return 1;
    }

    fd2 = open(argv[2], O_RDONLY);
    if (fd2 < 0) {
        printf("Cannot open %s\n", argv[2]);
        close(fd1);
        return 1;
    }

    if (fstat(fd1, &st1) < 0 || fstat(fd2, &st2) < 0) {
        printf("Cannot stat files\n");
        close(fd1);
        close(fd2);
        return 1;
    }

    if (st1.st_size != st2.st_size) {
        printf("Files have different sizes: %s=%ld, %s=%ld\n",
               argv[1], st1.st_size, argv[2], st2.st_size);
        close(fd1);
        close(fd2);
        return 1;
    }

    size = st1.st_size;
    if (size == 0) {
        printf("Files are empty and identical\n");
        close(fd1);
        close(fd2);
        return 0;
    }

    map1 = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd1, 0);
    map2 = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd2, 0);

    if (map1 == MAP_FAILED || map2 == MAP_FAILED) {
        printf("mmap failed\n");
        close(fd1);
        close(fd2);
        return 1;
    }

    /* First pass: collect 16-byte aligned row offsets that have differences */
    diff_row_offsets = malloc(diff_capacity * sizeof(size_t));
    if (!diff_row_offsets) {
        printf("Memory allocation failed\n");
        munmap(map1, size);
        munmap(map2, size);
        close(fd1);
        close(fd2);
        return 1;
    }

    for (i = 0; i < size; i += 16) {
        size_t row_end = i + 16;
        size_t j;
        int has_diff = 0;

        if (row_end > size)
            row_end = size;

        for (j = i; j < row_end; j++) {
            if (map1[j] != map2[j]) {
                has_diff = 1;
                break;
            }
        }

        if (has_diff) {
            if (diff_row_count >= diff_capacity) {
                diff_capacity *= 2;
                diff_row_offsets = realloc(diff_row_offsets, diff_capacity * sizeof(size_t));
                if (!diff_row_offsets) {
                    printf("Memory allocation failed\n");
                    munmap(map1, size);
                    munmap(map2, size);
                    close(fd1);
                    close(fd2);
                    return 1;
                }
            }
            diff_row_offsets[diff_row_count++] = i;
        }
    }

    if (diff_row_count == 0) {
        printf("Files are identical\n");
        free(diff_row_offsets);
        munmap(map1, size);
        munmap(map2, size);
        close(fd1);
        close(fd2);
        return 0;
    }

    printf("Found %zu rows with differences\n\n", diff_row_count);

    term_height = get_terminal_height();
    /* Each diff row takes 2 lines (file1 + file2), plus header and prompt */
    page_size = (term_height - 4) / 2;
    if (page_size < 1)
        page_size = 1;

    /* Paginated display */
    while (1) {
        size_t end_pos;
        size_t j;

        /* Clear screen */
        printf("\033[2J\033[H");

        printf("Offset         +0       +4       +8       +C\n");
        printf("----------------------------------------------\n");

        end_pos = current_pos + page_size;
        if (end_pos > diff_row_count)
            end_pos = diff_row_count;

        for (j = current_pos; j < end_pos; j++) {
            size_t offset = diff_row_offsets[j];
            size_t row_len = 16;
            if (offset + 16 > size)
                row_len = size - offset;
            print_row(offset, &map1[offset], &map2[offset], row_len);
        }

        printf("\n-- %zu-%zu of %zu rows -- SPACE/PgDn: next, PgUp: prev, q: quit --",
               current_pos + 1, end_pos, diff_row_count);
        fflush(stdout);

        key = get_key();

        if (key == KEY_QUIT || key == 'Q') {
            break;
        } else if (key == KEY_SPACE || key == KEY_PGDN) {
            if (current_pos + page_size < diff_row_count)
                current_pos += page_size;
        } else if (key == KEY_PGUP) {
            if (current_pos >= page_size)
                current_pos -= page_size;
            else
                current_pos = 0;
        }
    }

    printf("\n");

    free(diff_row_offsets);
    munmap(map1, size);
    munmap(map2, size);
    close(fd1);
    close(fd2);

    return 0;
}
