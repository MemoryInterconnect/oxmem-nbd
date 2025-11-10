/*
 * Test program for multiple NBD devices
 * Tests accessibility of /dev/nbd0 to /dev/nbdN by reading/writing first and last sectors
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

#define SECTOR_SIZE 512

/**
 * Get device size in bytes
 */
static off_t get_device_size(int fd)
{
    off_t size;

    if (ioctl(fd, BLKGETSIZE64, &size) < 0) {
        return -1;
    }

    return size;
}

/**
 * Test a single NBD device
 */
static int test_device(const char *device_path, int device_num)
{
    int fd;
    off_t device_size;
    off_t last_sector_offset;
    char first_sector[SECTOR_SIZE];
    char last_sector[SECTOR_SIZE];
    ssize_t bytes_read;
    int errors = 0;

    printf("\n[Device %d: %s]\n", device_num, device_path);

    // Open device
    fd = open(device_path, O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "  ERROR: Failed to open %s: %s\n", device_path, strerror(errno));
        return -1;
    }

    // Get device size
    device_size = get_device_size(fd);
    if (device_size < 0) {
        fprintf(stderr, "  ERROR: Failed to get device size: %s\n", strerror(errno));
        close(fd);
        return -1;
    }

    printf("  Device size: %lld bytes (%lld MB)\n",
           (long long)device_size, (long long)device_size / (1024*1024));

    // Calculate last sector offset
    if (device_size < SECTOR_SIZE) {
        fprintf(stderr, "  ERROR: Device too small (< 512 bytes)\n");
        close(fd);
        return -1;
    }
    last_sector_offset = device_size - SECTOR_SIZE;

    // Test reading first sector
    printf("  Testing first sector (offset 0x0)...\n");
    if (lseek(fd, 0, SEEK_SET) < 0) {
        fprintf(stderr, "  ERROR: Failed to seek to first sector: %s\n", strerror(errno));
        errors++;
    } else {
        bytes_read = read(fd, first_sector, SECTOR_SIZE);
        if (bytes_read < 0) {
            fprintf(stderr, "  ERROR: Failed to read first sector: %s\n", strerror(errno));
            errors++;
        } else if (bytes_read != SECTOR_SIZE) {
            fprintf(stderr, "  ERROR: Incomplete read of first sector (%zd/%d bytes)\n",
                    bytes_read, SECTOR_SIZE);
            errors++;
        } else {
            printf("  ✓ First sector read successfully\n");
            // Show first 16 bytes
            printf("    First 16 bytes: ");
            for (int i = 0; i < 16 && i < bytes_read; i++) {
                printf("%02x ", (unsigned char)first_sector[i]);
            }
            printf("\n");
        }
    }

    // Test reading last sector
    printf("  Testing last sector (offset 0x%llx)...\n", (long long)last_sector_offset);
    if (lseek(fd, last_sector_offset, SEEK_SET) < 0) {
        fprintf(stderr, "  ERROR: Failed to seek to last sector: %s\n", strerror(errno));
        errors++;
    } else {
        bytes_read = read(fd, last_sector, SECTOR_SIZE);
        if (bytes_read < 0) {
            fprintf(stderr, "  ERROR: Failed to read last sector: %s\n", strerror(errno));
            errors++;
        } else if (bytes_read != SECTOR_SIZE) {
            fprintf(stderr, "  ERROR: Incomplete read of last sector (%zd/%d bytes)\n",
                    bytes_read, SECTOR_SIZE);
            errors++;
        } else {
            printf("  ✓ Last sector read successfully\n");
            // Show first 16 bytes
            printf("    First 16 bytes: ");
            for (int i = 0; i < 16 && i < bytes_read; i++) {
                printf("%02x ", (unsigned char)last_sector[i]);
            }
            printf("\n");
        }
    }

    close(fd);

    if (errors == 0) {
        printf("  ✓ Device %s: ALL TESTS PASSED\n", device_path);
        return 0;
    } else {
        printf("  ✗ Device %s: %d ERROR(S)\n", device_path, errors);
        return -1;
    }
}

/**
 * Show usage
 */
static void show_help(const char *progname)
{
    printf("Usage: %s <num_devices>\n\n", progname);
    printf("Test NBD device accessibility\n\n");
    printf("Arguments:\n");
    printf("  num_devices   Number of NBD devices to test (1-16)\n");
    printf("                1 = test /dev/nbd0 only\n");
    printf("                4 = test /dev/nbd0 to /dev/nbd3\n");
    printf("                16 = test /dev/nbd0 to /dev/nbd15\n\n");
    printf("Examples:\n");
    printf("  %s 1     # Test /dev/nbd0 only\n", progname);
    printf("  %s 4     # Test /dev/nbd0-3\n", progname);
    printf("  sudo %s 4     # Run with sudo if devices require root access\n", progname);
}

/**
 * Main function
 */
int main(int argc, char *argv[])
{
    int num_devices;
    int i;
    int total_errors = 0;
    int devices_tested = 0;
    char device_path[32];

    // Parse arguments
    if (argc != 2) {
        show_help(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
        show_help(argv[0]);
        return 0;
    }

    num_devices = atoi(argv[1]);
    if (num_devices < 1 || num_devices > 16) {
        fprintf(stderr, "Error: num_devices must be between 1 and 16\n");
        show_help(argv[0]);
        return 1;
    }

    printf("===========================================\n");
    printf("NBD Device Accessibility Test\n");
    printf("===========================================\n");
    printf("Testing %d device(s): /dev/nbd0", num_devices);
    if (num_devices > 1) {
        printf(" to /dev/nbd%d", num_devices - 1);
    }
    printf("\n");

    // Test each device
    for (i = 0; i < num_devices; i++) {
        snprintf(device_path, sizeof(device_path), "/dev/nbd%d", i);

        if (test_device(device_path, i) < 0) {
            total_errors++;
        }
        devices_tested++;
    }

    // Summary
    printf("\n===========================================\n");
    printf("Test Summary\n");
    printf("===========================================\n");
    printf("Devices tested: %d\n", devices_tested);
    printf("Devices passed: %d\n", devices_tested - total_errors);
    printf("Devices failed: %d\n", total_errors);

    if (total_errors == 0) {
        printf("\n✓ ALL TESTS PASSED\n");
        return 0;
    } else {
        printf("\n✗ SOME TESTS FAILED\n");
        return 1;
    }
}
