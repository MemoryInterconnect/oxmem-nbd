/*
 * OmniXtend Memory Block Device Driver - Simplified Version
 * Simple userspace block device using NBD with blocking I/O
 * No queues, no separate send/recv threads - just simple blocking operations
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <linux/if_packet.h>
#include <linux/nbd.h>
#include <net/ethernet.h>
#include <net/if.h>
#include <fcntl.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <signal.h>
#include <sys/time.h>
#include "lib-ox-packet.h"

// NBD device path
#define NBD_DEVICE "/dev/nbd0"
#define READ_WRITE_UNIT 1024
#define RESPONSE_TIMEOUT_SEC 10

// Global state structure - much simpler!
struct oxmem_bdev {
    // Network
    struct oxmem_info_struct oxmem_info;

    // NBD
    int nbd_fd;
    int sk_pair[2];

    unsigned int credit_in_use[5];
};

static struct oxmem_bdev bdev;
static volatile sig_atomic_t shutdown_requested = 0;

// Function declarations
void *nbd_server_thread(void *arg);
static void signal_handler(int sig);
static void cleanup_and_exit(void);
static int oxmem_read_blocking(char *buf, size_t size, off_t offset);
static int oxmem_write_blocking(const char *buf, size_t size, off_t offset);
static int send_and_wait_response(struct ox_packet_struct *send_ox,
                                   struct ox_packet_struct *recv_ox,
                                   int expect_data);

/**
 * Signal handler for graceful shutdown
 */
static void signal_handler(int sig)
{
    (void)sig;
    shutdown_requested = 1;
    if (bdev.nbd_fd > 0) {
        ioctl(bdev.nbd_fd, NBD_DISCONNECT);
    }
}

int get_ack_credit(int channel)
{
	int i;

	if ( channel < CHANNEL_A || channel > CHANNEL_E ) 
		return 0;

        // Round down to power of 2
        for (i = 31; i >= 0; i--) {
            if (bdev.credit_in_use[channel - 1] >= (1UL << i)) {
                break;
            }
        }

	if ( i >= 0 )
		//reduce credit_in_use
		bdev.credit_in_use[channel - 1] -= (1UL << i);

	return i;

}


int get_used_credit(struct ox_packet_struct * recv_ox, int channel)
{
	int credit = 0;
	int i;
    	struct tl_msg_header_chan_AD tl_msg_ack;
    	uint64_t be64_temp, tl_msg_mask;

	tl_msg_mask = recv_ox->tl_msg_mask;

    	for (i = 0; i < 64; i++) {
    	    if (tl_msg_mask & 0x1) {
        	    credit ++;
		    be64_temp = be64toh(recv_ox->flits[i]);
		    memcpy(&tl_msg_ack, &be64_temp, sizeof(uint64_t));
		    if ( tl_msg_ack.chan == channel && 
			tl_msg_ack.opcode == D_ACCESSACKDATA_OPCODE ) {
			    if ( tl_msg_ack.size >= 3 )
	    			credit += (1 << (tl_msg_ack.size-3));
			    else 
				credit ++;
		    }
	    }

	    tl_msg_mask = tl_msg_mask >> 1;
	    if ( tl_msg_mask == 0 ) break;
	}

	return credit;
}


/**
 * Send OmniXtend packet and wait for response (blocking with timeout)
 * This replaces the entire queue system!
 */
static int send_and_wait_response(struct ox_packet_struct *send_ox,
                                   struct ox_packet_struct *recv_ox,
                                   int expect_data)
{
    char send_buffer[BUFFER_SIZE];
    char recv_buffer[BUFFER_SIZE];
    int send_size, recv_size;
    fd_set readfds;
    struct timeval tv;
    int ret;
    int credit;

    // Set sequence number, credit and convert to packet
    set_seq_num_to_ox_packet(bdev.oxmem_info.connection_id, send_ox);

    if ( send_ox->tloe_hdr.chan == 0) {
	    credit = get_ack_credit(CHANNEL_D);
	    if ( credit >= 0 ) {
		    send_ox->tloe_hdr.credit = credit;
	    	    send_ox->tloe_hdr.chan = CHANNEL_D;
	    }
    }

    ox_struct_to_packet(send_ox, send_buffer, &send_size);

    // Send packet
    ret = send(bdev.oxmem_info.sockfd, send_buffer, send_size, 0);
    if (ret < 0) {
        perror("send failed");
        return -EIO;
    }

    // Wait for response with timeout
retry:
    FD_ZERO(&readfds);
    FD_SET(bdev.oxmem_info.sockfd, &readfds);
    tv.tv_sec = RESPONSE_TIMEOUT_SEC;
    tv.tv_usec = 0;

    ret = select(bdev.oxmem_info.sockfd + 1, &readfds, NULL, NULL, &tv);
    if (ret <= 0) {
        if (ret == 0)
            fprintf(stderr, "Timeout waiting for OmniXtend response\n");
        else
            perror("select failed");
        return -ETIMEDOUT;
    }

    // Receive response
    recv_size = recv(bdev.oxmem_info.sockfd, recv_buffer, BUFFER_SIZE, 0);
    if (recv_size < 0) {
        perror("recv failed");
        return -EIO;
    }

    // Check ethertype
    struct ethhdr *etherHeader = (struct ethhdr *)recv_buffer;
    if (etherHeader->h_proto != OX_ETHERTYPE) {
        fprintf(stderr, "Received non-OmniXtend packet\n");
//        return -EIO;
	goto retry;
    }

    // Parse response
    packet_to_ox_struct(recv_buffer, recv_size, recv_ox);

    // Update expected sequence number
    update_seq_num_expected(bdev.oxmem_info.connection_id, recv_ox);

    if (expect_data == 1 && recv_ox->tl_msg_mask == 0) 
	    goto retry;

    bdev.credit_in_use[CHANNEL_D-1] += get_used_credit(recv_ox, CHANNEL_D);

    return 0;
}

/**
 * Copy received data from OmniXtend response
 */
static int copy_data_from_response(char *target_buf, struct ox_packet_struct *recv_ox)
{
    uint64_t be64_temp;
    struct tl_msg_header_chan_AD tl_msg_ack;
    int i;
    uint64_t tl_msg_mask = recv_ox->tl_msg_mask;

    if (tl_msg_mask == 0) {
        fprintf(stderr, "No TileLink message in response\n");
        return -1;
    }

    // Find start of valid flit
    for (i = 0; i < 64; i++) {
        if ((tl_msg_mask >> i) & 0x1)
            break;
    }

    // Extract header and copy data
    be64_temp = be64toh(recv_ox->flits[i]);
    memcpy(&tl_msg_ack, &be64_temp, sizeof(uint64_t));
    memcpy(target_buf, &recv_ox->flits[i+1], 1 << tl_msg_ack.size);

    return 1 << tl_msg_ack.size;
}

/**
 * Read from OmniXtend memory (blocking, no queues)
 */
static int oxmem_read_blocking(char *buf, size_t size, off_t offset)
{
    struct ox_packet_struct send_ox, recv_ox;
    uint64_t send_flits[256];
    size_t bytes_read = 0;
    size_t chunk_size;
    int ret, i;

    // Bounds check
    if (offset + size > bdev.oxmem_info.st.st_size) {
        fprintf(stderr, "Read out of bounds\n");
        return -EINVAL;
    }

    // Connection check
    if (bdev.oxmem_info.connection_id < 0) {
        fprintf(stderr, "Not connected\n");
        return -ENXIO;
    }

    // Read in chunks
    while (bytes_read < size) {
        chunk_size = size - bytes_read;
        if (chunk_size > READ_WRITE_UNIT)
            chunk_size = READ_WRITE_UNIT;

        // Round down to power of 2
        for (i = 10; i >= 0; i--) {
            if (chunk_size >= (1 << i)) {
                chunk_size = 1 << i;
                break;
            }
        }

        // Prepare Get request
        memset(send_flits, 0, sizeof(send_flits));
        make_get_op_packet(bdev.oxmem_info.connection_id, chunk_size,
                          offset + bytes_read, &send_ox, send_flits);

        // Send and wait for response (blocking)
        ret = send_and_wait_response(&send_ox, &recv_ox, 1);
        if (ret < 0) {
            fprintf(stderr, "Get request failed at offset %ld\n", bytes_read);
            return ret;
        }

        // Copy data from response
        ret = copy_data_from_response(buf + bytes_read, &recv_ox);
        if (ret < 0) {
            fprintf(stderr, "Failed to extract data from response\n");
            return -EIO;
        }

        bytes_read += chunk_size;
    }

    return size;
}

/**
 * Write to OmniXtend memory (blocking, no queues)
 */
static int oxmem_write_blocking(const char *buf, size_t size, off_t offset)
{
    struct ox_packet_struct send_ox, recv_ox;
    uint64_t send_flits[256];
    size_t bytes_written = 0;
    size_t chunk_size;
    int ret, i;

    // Bounds check
    if (offset + size > bdev.oxmem_info.st.st_size) {
        fprintf(stderr, "Write out of bounds\n");
        return -EINVAL;
    }

    // Write in chunks
    while (bytes_written < size) {
        chunk_size = size - bytes_written;
        if (chunk_size > READ_WRITE_UNIT)
            chunk_size = READ_WRITE_UNIT;

        // Round down to power of 2
        for (i = 10; i >= 0; i--) {
            if (chunk_size >= (1 << i)) {
                chunk_size = 1 << i;
                break;
            }
        }

        // Prepare PutFullData request
        make_putfull_op_packet(bdev.oxmem_info.connection_id,
                              buf + bytes_written, chunk_size,
                              offset + bytes_written, &send_ox, send_flits);

        // Send and wait for acknowledgment (blocking)
        ret = send_and_wait_response(&send_ox, &recv_ox, 1);
        if (ret < 0) {
            fprintf(stderr, "Put request failed at offset %ld\n", bytes_written);
            return ret;
        }

        bytes_written += chunk_size;
    }

    return size;
}

/**
 * NBD server thread - handles NBD requests
 * This is the ONLY thread now!
 */
void *nbd_server_thread(void *arg)
{
    struct nbd_request request;
    struct nbd_reply reply;
    char *buffer;
    ssize_t bytes_read;

    (void)arg;

    buffer = malloc(1024 * 1024); // 1MB buffer
    if (!buffer) {
        fprintf(stderr, "Failed to allocate buffer\n");
        return NULL;
    }

    while (!shutdown_requested) {
        // Read NBD request
        bytes_read = read(bdev.sk_pair[1], &request, sizeof(request));

        if (bytes_read == 0) {
            printf("NBD connection closed\n");
            break;
        }

        if (bytes_read < 0) {
            if (errno == EINTR) continue;
            perror("Failed to read NBD request");
            break;
        }

        if (bytes_read != sizeof(request)) {
            fprintf(stderr, "Incomplete NBD request\n");
            continue;
        }

        // Parse request
        uint32_t type = ntohl(request.type);
        uint64_t offset = be64toh(*(uint64_t*)&request.from);
        uint32_t len = ntohl(request.len);

        // Prepare reply
        memset(&reply, 0, sizeof(reply));
        reply.magic = htonl(NBD_REPLY_MAGIC);
        memcpy(reply.handle, request.handle, sizeof(reply.handle));
        reply.error = 0;

        // Handle request
        switch (type) {
        case NBD_CMD_READ: {
            int result = oxmem_read_blocking(buffer, len, offset);

            if (result > 0) {
                reply.error = 0;
                write(bdev.sk_pair[1], &reply, sizeof(reply));
                write(bdev.sk_pair[1], buffer, len);
            } else {
                reply.error = htonl(EIO);
                write(bdev.sk_pair[1], &reply, sizeof(reply));
            }
            break;
        }

        case NBD_CMD_WRITE: {
            // Read write data
            ssize_t total_bytes = 0;
            do {
                bytes_read = read(bdev.sk_pair[1], buffer + total_bytes, len - total_bytes);
                if (bytes_read < 0) {
                    reply.error = htonl(EIO);
                    write(bdev.sk_pair[1], &reply, sizeof(reply));
                    break;
                }
                total_bytes += bytes_read;
            } while (total_bytes < len);

            if (total_bytes < len) break;

            int result = oxmem_write_blocking(buffer, len, offset);

            if (result > 0) {
                reply.error = 0;
            } else {
                reply.error = htonl(EIO);
            }

            write(bdev.sk_pair[1], &reply, sizeof(reply));
            break;
        }

        case NBD_CMD_DISC:
            printf("NBD disconnect requested\n");
            goto cleanup;

        case NBD_CMD_FLUSH:
            // No-op for network device
            write(bdev.sk_pair[1], &reply, sizeof(reply));
            break;

        default:
            printf("Unknown NBD command: %u\n", type);
            reply.error = htonl(EINVAL);
            write(bdev.sk_pair[1], &reply, sizeof(reply));
            break;
        }
    }

cleanup:
    free(buffer);
    printf("NBD server thread exiting\n");
    return NULL;
}

/**
 * Cleanup and exit
 */
static void cleanup_and_exit(void)
{
    struct ox_packet_struct send_ox, recv_ox;

    // Send disconnect packet
    if (bdev.oxmem_info.connection_id >= 0) {
        printf("\nClosing OmniXtend connection...\n");

        make_close_connection_packet(bdev.oxmem_info.connection_id, &send_ox);

        // Try to send disconnect (ignore errors during shutdown)
        send_and_wait_response(&send_ox, &recv_ox, 0);

        delete_connection(bdev.oxmem_info.connection_id);
        bdev.oxmem_info.connection_id = -1;
    }

    // Cleanup NBD
    if (bdev.nbd_fd > 0) {
        ioctl(bdev.nbd_fd, NBD_CLEAR_QUE);
        ioctl(bdev.nbd_fd, NBD_CLEAR_SOCK);
        close(bdev.nbd_fd);
    }

    // Close sockets
    if (bdev.sk_pair[0] > 0) {
        close(bdev.sk_pair[0]);
        close(bdev.sk_pair[1]);
    }

    if (bdev.oxmem_info.sockfd > 0) {
        close(bdev.oxmem_info.sockfd);
    }

    printf("Cleanup complete\n");
}

/**
 * Parse size with unit (G/M/K)
 */
static size_t parse_size_with_unit(const char *size_str)
{
    if (!size_str) return 0;

    char *end;
    unsigned long long value = strtoull(size_str, &end, 10);
    if (end == size_str) return 0;

    size_t multiplier = 1;
    if (*end != '\0') {
        switch (*end) {
        case 'G': case 'g':
            multiplier = 1024ULL * 1024ULL * 1024ULL;
            break;
        case 'M': case 'm':
            multiplier = 1024ULL * 1024ULL;
            break;
        case 'K': case 'k':
            multiplier = 1024ULL;
            break;
        default:
            return 0;
        }
    }

    return (size_t)(value * multiplier);
}

/**
 * Show usage
 */
static void show_help(const char *progname)
{
    printf("Usage: %s [options]\n\n", progname);
    printf("Simple OmniXtend block device driver\n\n");
    printf("Required options:\n"
           "  --netdev=DEV    Network interface (REQUIRED)\n"
           "  --mac=MAC       MAC address of OX endpoint (REQUIRED)\n"
           "  --size=SIZE     Size with unit G/M/K (REQUIRED)\n\n"
           "Optional options:\n"
           "  --base=ADDR     Base address in hex (default: 0x0)\n"
           "  -h, --help      Show this help\n\n"
           "Example:\n"
           "  %s --netdev=veth1 --mac=04:00:00:00:00:00 --size=1G\n",
           progname);
}

/**
 * Main function
 */
int main(int argc, char *argv[])
{
    char *netdev = NULL;
    char *mac_str = NULL;
    char *size_str = NULL;
    char *base_str = "0x0";
    uint32_t mac_values[6];
    struct sockaddr_ll saddr;
    struct ox_packet_struct send_ox, recv_ox;
    pthread_t nbd_thread;
    int i;

    // Parse arguments
    for (i = 1; i < argc; i++) {
        if (strncmp(argv[i], "--netdev=", 9) == 0) {
            netdev = argv[i] + 9;
        } else if (strncmp(argv[i], "--mac=", 6) == 0) {
            mac_str = argv[i] + 6;
        } else if (strncmp(argv[i], "--size=", 7) == 0) {
            size_str = argv[i] + 7;
        } else if (strncmp(argv[i], "--base=", 7) == 0) {
            base_str = argv[i] + 7;
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            show_help(argv[0]);
            return 0;
        }
    }

    // Validate
    if (!netdev || !mac_str || !size_str) {
        fprintf(stderr, "Error: Missing required arguments\n");
        show_help(argv[0]);
        return 1;
    }

    // Setup signal handlers
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    // Initialize
    memset(&bdev, 0, sizeof(bdev));
    strncpy(bdev.oxmem_info.netdev, netdev, sizeof(bdev.oxmem_info.netdev) - 1);
    bdev.oxmem_info.netdev_id = if_nametoindex(netdev);

    if (bdev.oxmem_info.netdev_id == 0) {
        fprintf(stderr, "Error: Invalid network device: %s\n", netdev);
        return 1;
    }

    // Parse MAC
    if (sscanf(mac_str, "%2x:%2x:%2x:%2x:%2x:%2x",
               &mac_values[0], &mac_values[1], &mac_values[2],
               &mac_values[3], &mac_values[4], &mac_values[5]) != 6) {
        fprintf(stderr, "Error: Invalid MAC address\n");
        return 1;
    }

    for (i = 0; i < 6; i++) {
        bdev.oxmem_info.mac_addr += (uint64_t)mac_values[i] << (i * 8);
    }

    // Parse base and size
    bdev.oxmem_info.base = strtoull(base_str, NULL, 16);
    size_t parsed_size = parse_size_with_unit(size_str);
    if (parsed_size == 0) {
        fprintf(stderr, "Error: Invalid size\n");
        return 1;
    }

    bdev.oxmem_info.st.st_size = bdev.oxmem_info.size = parsed_size;
    bdev.oxmem_info.connection_id = -1;

    printf("OmniXtend Block Device (Simplified):\n");
    printf("  Network: %s\n", netdev);
    printf("  MAC: %s\n", mac_str);
    printf("  Size: %zu bytes (%zu MB)\n", parsed_size, parsed_size / (1024*1024));
    printf("  Base: 0x%lx\n", bdev.oxmem_info.base);

    // Create socket
    bdev.oxmem_info.sockfd = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL));
    if (bdev.oxmem_info.sockfd < 0) {
        perror("Socket creation failed");
        return 1;
    }

    // Bind socket
    memset(&saddr, 0, sizeof(saddr));
    saddr.sll_family = AF_PACKET;
    saddr.sll_protocol = htons(ETH_P_ALL);
    saddr.sll_ifindex = bdev.oxmem_info.netdev_id;

    if (bind(bdev.oxmem_info.sockfd, (struct sockaddr *)&saddr, sizeof(saddr)) < 0) {
        perror("Socket bind failed");
        close(bdev.oxmem_info.sockfd);
        return 1;
    }

    // Open connection (simplified - blocking)
    printf("Establishing OmniXtend connection...\n");

    bdev.oxmem_info.connection_id =
        make_open_connection_packet(bdev.oxmem_info.sockfd,
                                   bdev.oxmem_info.netdev,
                                   bdev.oxmem_info.mac_addr,
                                   &send_ox);

    send_ox.tloe_hdr.chan = CHANNEL_A;
    send_ox.tloe_hdr.credit = 9;
    // Send on Channel A (blocking)
    send_and_wait_response(&send_ox, &recv_ox, 0);

    // Send on Channel D (blocking)
    send_ox.tloe_hdr.msg_type = NORMAL;
    send_ox.tloe_hdr.chan = CHANNEL_D;
    send_ox.tloe_hdr.credit = 9;
    send_and_wait_response(&send_ox, &recv_ox, 0);

    printf("Connection established (ID: %d)\n", bdev.oxmem_info.connection_id);

    // Open NBD device
    bdev.nbd_fd = open(NBD_DEVICE, O_RDWR);
    if (bdev.nbd_fd < 0) {
        perror("Failed to open NBD device");
        fprintf(stderr, "Hint: sudo modprobe nbd\n");
        cleanup_and_exit();
        return 1;
    }

    // Create socketpair
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, bdev.sk_pair) < 0) {
        perror("Failed to create socketpair");
        close(bdev.nbd_fd);
        cleanup_and_exit();
        return 1;
    }

    // Configure NBD
    if (ioctl(bdev.nbd_fd, NBD_SET_SIZE, bdev.oxmem_info.size) < 0) {
        perror("NBD_SET_SIZE failed");
        cleanup_and_exit();
        return 1;
    }

    ioctl(bdev.nbd_fd, NBD_CLEAR_SOCK);
    ioctl(bdev.nbd_fd, NBD_SET_BLKSIZE, 4096);
    ioctl(bdev.nbd_fd, NBD_SET_TIMEOUT, 10);

    if (ioctl(bdev.nbd_fd, NBD_SET_SOCK, bdev.sk_pair[0]) < 0) {
        perror("NBD_SET_SOCK failed");
        cleanup_and_exit();
        return 1;
    }

    printf("Block device ready at %s\n", NBD_DEVICE);
    printf("Press Ctrl+C to stop...\n");
    fflush(stdout);

    // Start NBD thread (only thread!)
    if (pthread_create(&nbd_thread, NULL, nbd_server_thread, NULL) != 0) {
        perror("Failed to create NBD thread");
        cleanup_and_exit();
        return 1;
    }

    usleep(100000); // Small delay

    // Run NBD (blocks until disconnect)
    if (ioctl(bdev.nbd_fd, NBD_DO_IT) < 0) {
        if (!shutdown_requested) {
            perror("NBD_DO_IT failed");
        }
    }

    // Wait for thread
    pthread_join(nbd_thread, NULL);

    // Cleanup
    cleanup_and_exit();
    return 0;
}
