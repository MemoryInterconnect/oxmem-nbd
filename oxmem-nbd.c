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
#define MAX_NBD_DEVICES 16
#define MAX_SEND_OX_LIST 1000

// Per-NBD device state
struct oxmem_bdev {
    // NBD
    int nbd_fd;
    int sk_pair[2];

    // Device-specific info
    size_t device_size;
    off_t device_base;
    int device_index;
    char device_path[32];
};

// Global state
struct oxmem_global {
    // Network (shared across all NBD devices)
    struct oxmem_info_struct oxmem_info;
    unsigned int credit_in_use[5];

    // Multiple NBD devices
    struct oxmem_bdev bdevs[MAX_NBD_DEVICES];
    int num_devices;
};

struct send_ox_list_entry {
    sem_t *sem;
    char *recv_buffer;
    int *recv_size;
};
struct send_ox_list_entry send_ox_list[MAX_SEND_OX_LIST];
static pthread_mutex_t send_ox_list_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t credit_lock = PTHREAD_MUTEX_INITIALIZER;

static struct oxmem_global global_state;
static volatile sig_atomic_t shutdown_requested = 0;

// Function declarations
void *nbd_server_thread(void *arg);
void *recv_thread(void *arg);
static void signal_handler(int sig);
static void cleanup_and_exit(void);
static int oxmem_read_blocking(char *buf, size_t size, off_t offset,
			       struct oxmem_bdev *bdev);
static int oxmem_write_blocking(const char *buf, size_t size, off_t offset,
				struct oxmem_bdev *bdev);
static int send_and_wait_response(struct ox_packet_struct *send_ox,
				  struct ox_packet_struct *recv_ox,
				  int expect_data);
static int copy_data_from_response(char *target_buf,
				   struct ox_packet_struct *recv_ox);
/**
 * Signal handler for graceful shutdown
 */
static void signal_handler(int sig)
{
    int i;
    (void) sig;
    shutdown_requested = 1;

    // Cleanup
    cleanup_and_exit();

    for (i = 0; i < global_state.num_devices; i++) {
	if (global_state.bdevs[i].nbd_fd > 0) {
	    ioctl(global_state.bdevs[i].nbd_fd, NBD_DISCONNECT);
	}
    }

}

int get_ack_credit(int channel)
{
    int i;
    int credit;

    if (channel < CHANNEL_A || channel > CHANNEL_E)
	return 0;

    pthread_mutex_lock(&credit_lock);

    // Round down to power of 2
    for (i = 31; i >= 0; i--) {
	if (global_state.credit_in_use[channel - 1] >= (1UL << i)) {
	    break;
	}
    }

    credit = i;

    if (i >= 0)
	//reduce credit_in_use
	global_state.credit_in_use[channel - 1] -= (1UL << i);

    pthread_mutex_unlock(&credit_lock);

    return credit;

}


int get_used_credit(struct ox_packet_struct *recv_ox, int channel)
{
    int credit = 0;
    int i;
    struct tl_msg_header_chan_AD tl_msg_ack;
    uint64_t be64_temp, tl_msg_mask;

    tl_msg_mask = recv_ox->tl_msg_mask;

    for (i = 0; i < 64; i++) {
	if (tl_msg_mask & 0x1) {
	    credit++;
	    be64_temp = be64toh(recv_ox->flits[i]);
	    memcpy(&tl_msg_ack, &be64_temp, sizeof(uint64_t));
	    if (tl_msg_ack.chan == channel &&
		tl_msg_ack.opcode == D_ACCESSACKDATA_OPCODE) {
		if (tl_msg_ack.size >= 3)
		    credit += (1 << (tl_msg_ack.size - 3));
		else
		    credit++;
	    }
	}

	tl_msg_mask = tl_msg_mask >> 1;
	if (tl_msg_mask == 0)
	    break;
    }

    return credit;
}

int get_send_ox_list(sem_t * sem, char *recv_buffer, int *recv_size)
{
    int source;

    pthread_mutex_lock(&send_ox_list_lock);

    for (source = 0; source < MAX_SEND_OX_LIST; source++) {
	if (send_ox_list[source].sem == NULL) {
	    send_ox_list[source].sem = sem;
	    send_ox_list[source].recv_buffer = recv_buffer;
	    send_ox_list[source].recv_size = recv_size;

	    pthread_mutex_unlock(&send_ox_list_lock);
	    return source;
	}
    }

    pthread_mutex_unlock(&send_ox_list_lock);
    return -1;
}

void free_send_ox_list(int source)
{
    pthread_mutex_lock(&send_ox_list_lock);
    send_ox_list[source].sem = NULL;
    send_ox_list[source].recv_buffer = NULL;
    send_ox_list[source].recv_size = NULL;
    pthread_mutex_unlock(&send_ox_list_lock);
}

void update_send_ox_credit(struct ox_packet_struct *send_ox)
{
    int credit;

    if (send_ox->tloe_hdr.chan == 0) {
	credit = get_ack_credit(CHANNEL_D);
	if (credit >= 0) {
	    send_ox->tloe_hdr.credit = credit;
	    send_ox->tloe_hdr.chan = CHANNEL_D;
	}
    }
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
    struct timeval tv;
    int ret;
    int credit;
    sem_t sem;
    int source = -1;
    struct tl_msg_header_chan_AD tl_msg_header;
    int64_t be64_temp;


    // Set sequence number, credit and convert to packet
    set_seq_num_to_ox_packet(global_state.oxmem_info.connection_id,
			     send_ox);

    update_send_ox_credit(send_ox);

    // Get a send_ox list slot
    if (expect_data) {
	sem_init(&sem, 0, 0);
	source = get_send_ox_list(&sem, recv_buffer, &recv_size);
	if (source < 0) {
	    printf("send_ox_list is full.\n");
	    exit(0);
	}
	// Set source field in TileLink message (only if TileLink message exists)
	if (send_ox->tl_msg_mask != 0 && send_ox->flits != NULL) {
	    // Find first valid flit
	    int i;
	    for (i = 0; i < 64; i++) {
		if ((send_ox->tl_msg_mask >> i) & 0x1)
		    break;
	    }

	    if (i < 64) {
		// Extract TileLink header, set source, write back
		be64_temp = be64toh(send_ox->flits[i]);
		memcpy(&tl_msg_header, &be64_temp, sizeof(uint64_t));
		tl_msg_header.source = source;
		be64_temp = htobe64(*(uint64_t *) & (tl_msg_header));
		send_ox->flits[i] = be64_temp;
	    }
	}
    }

    ox_struct_to_packet(send_ox, send_buffer, &send_size);

    // Send packet
    ret = send(global_state.oxmem_info.sockfd, send_buffer, send_size, 0);
    if (ret < 0) {
	perror("send failed");
	return -EIO;
    }

    if (expect_data) {
	// Wait for response
	sem_wait(&sem);

	// Parse response
	packet_to_ox_struct(recv_buffer, recv_size, recv_ox);

	pthread_mutex_lock(&credit_lock);
	global_state.credit_in_use[CHANNEL_D - 1] +=
	    get_used_credit(recv_ox, CHANNEL_D);
	pthread_mutex_unlock(&credit_lock);
    }
    // Free the source slot
    free_send_ox_list(source);
    sem_destroy(&sem);

    return 0;
}

static int send_and_wait_data(struct ox_packet_struct *send_ox,
				  char * data_buffer)
{
    char send_buffer[BUFFER_SIZE];
    char recv_buffer[BUFFER_SIZE];
    struct ox_packet_struct recv_ox;
    int send_size, recv_size;
    struct timeval tv;
    int ret;
    int credit;
    sem_t sem;
    int source = -1;
    struct tl_msg_header_chan_AD tl_msg_header;
    int64_t be64_temp;


    // Set sequence number, credit and convert to packet
    set_seq_num_to_ox_packet(global_state.oxmem_info.connection_id,
			     send_ox);

    update_send_ox_credit(send_ox);

    // Get a send_ox list slot
	sem_init(&sem, 0, 0);
	source = get_send_ox_list(&sem, recv_buffer, &recv_size);
	if (source < 0) {
	    printf("send_ox_list is full.\n");
	    exit(0);
	}
	// Set source field in TileLink message (only if TileLink message exists)
	if (send_ox->tl_msg_mask != 0 && send_ox->flits != NULL) {
	    // Find first valid flit
	    int i;
	    for (i = 0; i < 64; i++) {
		if ((send_ox->tl_msg_mask >> i) & 0x1)
		    break;
	    }

	    if (i < 64) {
		// Extract TileLink header, set source, write back
		be64_temp = be64toh(send_ox->flits[i]);
		memcpy(&tl_msg_header, &be64_temp, sizeof(uint64_t));
		tl_msg_header.source = source;
		be64_temp = htobe64(*(uint64_t *) & (tl_msg_header));
		send_ox->flits[i] = be64_temp;
	    }
	}

    ox_struct_to_packet(send_ox, send_buffer, &send_size);

    // Send packet
    ret = send(global_state.oxmem_info.sockfd, send_buffer, send_size, 0);
    if (ret < 0) {
	perror("send failed");
	return -EIO;
    }

	// Wait for response
	sem_wait(&sem);

	// Parse response
	packet_to_ox_struct(recv_buffer, recv_size, &recv_ox);

    ret = copy_data_from_response(data_buffer, &recv_ox);

    // Free the source slot
    free_send_ox_list(source);
    sem_destroy(&sem);

    return 0;
}

static int send_and_wait_ack(struct ox_packet_struct *send_ox)
{
    char send_buffer[BUFFER_SIZE];
    char recv_buffer[BUFFER_SIZE];
    int send_size, recv_size;
    struct timeval tv;
    int ret;
    int credit;
    sem_t sem;
    int source = -1;
    struct tl_msg_header_chan_AD tl_msg_header;
    int64_t be64_temp;


    // Set sequence number, credit and convert to packet
    set_seq_num_to_ox_packet(global_state.oxmem_info.connection_id,
			     send_ox);

    update_send_ox_credit(send_ox);

    // Get a send_ox list slot
	sem_init(&sem, 0, 0);
	source = get_send_ox_list(&sem, recv_buffer, &recv_size);
	if (source < 0) {
	    printf("send_ox_list is full.\n");
	    exit(0);
	}
	// Set source field in TileLink message (only if TileLink message exists)
	if (send_ox->tl_msg_mask != 0 && send_ox->flits != NULL) {
	    // Find first valid flit
	    int i;
	    for (i = 0; i < 64; i++) {
		if ((send_ox->tl_msg_mask >> i) & 0x1)
		    break;
	    }

	    if (i < 64) {
		// Extract TileLink header, set source, write back
		be64_temp = be64toh(send_ox->flits[i]);
		memcpy(&tl_msg_header, &be64_temp, sizeof(uint64_t));
		tl_msg_header.source = source;
		be64_temp = htobe64(*(uint64_t *) & (tl_msg_header));
		send_ox->flits[i] = be64_temp;
	    }
	}

    ox_struct_to_packet(send_ox, send_buffer, &send_size);

    // Send packet
    ret = send(global_state.oxmem_info.sockfd, send_buffer, send_size, 0);
    if (ret < 0) {
	perror("send failed");
	return -EIO;
    }

	// Wait for response
	sem_wait(&sem);

    // Free the source slot
    free_send_ox_list(source);
    sem_destroy(&sem);

    return 0;
}


/* send_and_forget - send ox_packet and don't care about reply */
static int send_and_forget(struct ox_packet_struct *send_ox)
{
    char send_buffer[BUFFER_SIZE];
    int send_size;
    int ret;

    // Set sequence number, credit and convert to packet
    set_seq_num_to_ox_packet(global_state.oxmem_info.connection_id,
			     send_ox);

    update_send_ox_credit(send_ox);

    ox_struct_to_packet(send_ox, send_buffer, &send_size);

    // Send packet
    ret = send(global_state.oxmem_info.sockfd, send_buffer, send_size, 0);
    if (ret < 0) {
	perror("send failed");
	return -EIO;
    }

    return 0;
}


void *recv_thread(void *arg)
{
    char recv_buffer[BUFFER_SIZE];
    int recv_size;
    struct ox_packet_struct recv_ox;
    struct tl_msg_header_chan_AD tl_msg_header;
    uint64_t be64_temp;
    int source;

    (void) arg;			// Unused parameter

    printf("Receive thread started\n");

    while (!shutdown_requested) {
	// Blocking receive
	recv_size = recv(global_state.oxmem_info.sockfd, recv_buffer,
			 BUFFER_SIZE, 0);

	if (recv_size < 0) {
	    if (shutdown_requested)
		break;
	    perror("recv failed");
	    continue;
	}
	// Check ethertype
	struct ethhdr *etherHeader = (struct ethhdr *) recv_buffer;
	if (etherHeader->h_proto != OX_ETHERTYPE) {
	    // Not an OmniXtend packet, ignore
	    continue;
	}
	// Parse response packet
	packet_to_ox_struct(recv_buffer, recv_size, &recv_ox);

	// Update expected sequence number
	update_seq_num_expected(global_state.oxmem_info.connection_id,
				&recv_ox);

	// Update credits
	pthread_mutex_lock(&credit_lock);
	global_state.credit_in_use[CHANNEL_D - 1] +=
	    get_used_credit(&recv_ox, CHANNEL_D);
	pthread_mutex_unlock(&credit_lock);

	// Extract source field from TileLink message to match request
	if (recv_ox.tl_msg_mask != 0) {
	    // Find first valid flit
	    int i;
	    for (i = 0; i < 64; i++) {
		if ((recv_ox.tl_msg_mask >> i) & 0x1)
		    break;
	    }

	    if (i < 64) {
		// Extract TileLink header
		be64_temp = be64toh(recv_ox.flits[i]);
		memcpy(&tl_msg_header, &be64_temp, sizeof(uint64_t));
		source = tl_msg_header.source;

		// Match and signal waiting thread
		pthread_mutex_lock(&send_ox_list_lock);
		if (source >= 0 && source < MAX_SEND_OX_LIST &&
		    send_ox_list[source].sem != NULL) {

		    // Copy response data
		    memcpy(send_ox_list[source].recv_buffer, recv_buffer,
			   recv_size);
		    *send_ox_list[source].recv_size = recv_size;

		    // Signal the waiting thread
		    sem_post(send_ox_list[source].sem);
		}
		pthread_mutex_unlock(&send_ox_list_lock);
	    }
	}
    }

    printf("Receive thread exiting\n");
    return NULL;
}

/**
 * Copy received data from OmniXtend response
 */
static int copy_data_from_response(char *target_buf,
				   struct ox_packet_struct *recv_ox)
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
    memcpy(target_buf, &recv_ox->flits[i + 1], 1 << tl_msg_ack.size);

    return 1 << tl_msg_ack.size;
}

/**
 * Read from OmniXtend memory (blocking, no queues)
 */
static int oxmem_read_blocking(char *buf, size_t size, off_t offset,
			       struct oxmem_bdev *bdev)
{
    struct ox_packet_struct send_ox, recv_ox;
    uint64_t send_flits[256];
    size_t bytes_read = 0;
    size_t chunk_size;
    int ret, i;
    off_t absolute_offset = bdev->device_base + offset;

    // Bounds check
    if (offset + size > bdev->device_size) {
	fprintf(stderr, "Read out of bounds (device %d)\n",
		bdev->device_index);
	return -EINVAL;
    }
    // Connection check
    if (global_state.oxmem_info.connection_id < 0) {
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
	make_get_op_packet(global_state.oxmem_info.connection_id,
			   chunk_size, absolute_offset + bytes_read,
			   &send_ox, send_flits);

	// Send and wait for response (blocking)
	ret = send_and_wait_data(&send_ox, buf + bytes_read);
	if (ret < 0) {
	    fprintf(stderr,
		    "Get request failed at offset %ld (device %d)\n",
		    bytes_read, bdev->device_index);
	    return ret;
	}

	// Copy data from response
//	ret = copy_data_from_response(buf + bytes_read, &recv_ox);
//	if (ret < 0) {
//	    fprintf(stderr, "Failed to extract data from response\n");
//	    return -EIO;
//	}

	bytes_read += chunk_size;
    }

    return size;
}

/**
 * Write to OmniXtend memory (blocking, no queues)
 */
static int oxmem_write_blocking(const char *buf, size_t size, off_t offset,
				struct oxmem_bdev *bdev)
{
    struct ox_packet_struct send_ox;
    uint64_t send_flits[256];
    size_t bytes_written = 0;
    size_t chunk_size;
    int ret, i;
    off_t absolute_offset = bdev->device_base + offset;

    // Bounds check
    if (offset + size > bdev->device_size) {
	fprintf(stderr, "Write out of bounds (device %d)\n",
		bdev->device_index);
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
	make_putfull_op_packet(global_state.oxmem_info.connection_id,
			       buf + bytes_written, chunk_size,
			       absolute_offset + bytes_written, &send_ox,
			       send_flits);

	// Send and wait for acknowledgment (blocking)
	ret = send_and_wait_ack(&send_ox);
	if (ret < 0) {
	    fprintf(stderr,
		    "Put request failed at offset %ld (device %d)\n",
		    bytes_written, bdev->device_index);
	    return ret;
	}

	bytes_written += chunk_size;
    }

    return size;
}

/**
 * NBD_DO_IT thread - one per device
 * This thread blocks in NBD_DO_IT ioctl until the device is disconnected
 */
void *nbd_do_it_thread(void *arg)
{
    struct oxmem_bdev *bdev = (struct oxmem_bdev *) arg;

    printf("NBD_DO_IT thread started for %s\n", bdev->device_path);

    // This blocks until NBD_DISCONNECT is called
    if (ioctl(bdev->nbd_fd, NBD_DO_IT) < 0) {
	if (!shutdown_requested) {
	    fprintf(stderr, "NBD_DO_IT failed for %s: %s\n",
		    bdev->device_path, strerror(errno));
	}
    }

    printf("NBD_DO_IT thread exiting for %s\n", bdev->device_path);
    return NULL;
}

/**
 * NBD server thread - handles NBD requests
 * One thread per NBD device
 */
void *nbd_server_thread(void *arg)
{
    struct oxmem_bdev *bdev = (struct oxmem_bdev *) arg;
    struct nbd_request request;
    struct nbd_reply reply;
    char *buffer;
    ssize_t bytes_read;

    buffer = malloc(1024 * 1024);	// 1MB buffer
    if (!buffer) {
	fprintf(stderr, "Failed to allocate buffer for device %d\n",
		bdev->device_index);
	return NULL;
    }

    printf("NBD server thread started for %s\n", bdev->device_path);

    while (!shutdown_requested) {
	// Read NBD request
	bytes_read = read(bdev->sk_pair[1], &request, sizeof(request));

	if (bytes_read == 0) {
	    printf("NBD connection closed for %s\n", bdev->device_path);
	    break;
	}

	if (bytes_read < 0) {
	    if (errno == EINTR)
		continue;
	    perror("Failed to read NBD request");
	    break;
	}

	if (bytes_read != sizeof(request)) {
	    fprintf(stderr, "Incomplete NBD request\n");
	    continue;
	}
	// Parse request
	uint32_t type = ntohl(request.type);
	uint64_t offset = be64toh(*(uint64_t *) & request.from);
	uint32_t len = ntohl(request.len);

	// Prepare reply
	memset(&reply, 0, sizeof(reply));
	reply.magic = htonl(NBD_REPLY_MAGIC);
	memcpy(reply.handle, request.handle, sizeof(reply.handle));
	reply.error = 0;

	// Handle request
	switch (type) {
	case NBD_CMD_READ:{
		int result =
		    oxmem_read_blocking(buffer, len, offset, bdev);

		if (result > 0) {
		    reply.error = 0;
		    write(bdev->sk_pair[1], &reply, sizeof(reply));
		    write(bdev->sk_pair[1], buffer, len);
		} else {
		    reply.error = htonl(EIO);
		    write(bdev->sk_pair[1], &reply, sizeof(reply));
		}
		break;
	    }

	case NBD_CMD_WRITE:{
		// Read write data
		ssize_t total_bytes = 0;
		do {
		    bytes_read =
			read(bdev->sk_pair[1], buffer + total_bytes,
			     len - total_bytes);
		    if (bytes_read < 0) {
			reply.error = htonl(EIO);
			write(bdev->sk_pair[1], &reply, sizeof(reply));
			break;
		    }
		    total_bytes += bytes_read;
		} while (total_bytes < len);

		if (total_bytes < len)
		    break;

		int result =
		    oxmem_write_blocking(buffer, len, offset, bdev);

		if (result > 0) {
		    reply.error = 0;
		} else {
		    reply.error = htonl(EIO);
		}

		write(bdev->sk_pair[1], &reply, sizeof(reply));
		break;
	    }

	case NBD_CMD_DISC:
	    printf("NBD disconnect requested for %s\n", bdev->device_path);
	    goto cleanup;

	case NBD_CMD_FLUSH:
	    // No-op for network device
	    write(bdev->sk_pair[1], &reply, sizeof(reply));
	    break;

	default:
	    printf("Unknown NBD command: %u\n", type);
	    reply.error = htonl(EINVAL);
	    write(bdev->sk_pair[1], &reply, sizeof(reply));
	    break;
	}
    }

  cleanup:
    free(buffer);
    printf("NBD server thread exiting for %s\n", bdev->device_path);
    return NULL;
}

/**
 * Cleanup and exit
 */
static void cleanup_and_exit(void)
{
    struct ox_packet_struct send_ox, recv_ox;
    int i;

    // Send disconnect packet
    if (global_state.oxmem_info.connection_id >= 0) {
	printf("\nClosing OmniXtend connection...\n");

	make_close_connection_packet(global_state.oxmem_info.connection_id,
				     &send_ox);

	// Try to send disconnect (ignore errors during shutdown)
	send_and_forget(&send_ox);

	delete_connection(global_state.oxmem_info.connection_id);
	global_state.oxmem_info.connection_id = -1;
    }
    // Cleanup all NBD devices
    for (i = 0; i < global_state.num_devices; i++) {
	struct oxmem_bdev *bdev = &global_state.bdevs[i];

	printf("\nClosing NBD sockets...\n");

	if (bdev->nbd_fd > 0) {
	    ioctl(bdev->nbd_fd, NBD_CLEAR_QUE);
	    ioctl(bdev->nbd_fd, NBD_CLEAR_SOCK);
	    close(bdev->nbd_fd);
	}
	// Close sockets
	if (bdev->sk_pair[0] > 0) {
	    close(bdev->sk_pair[0]);
	    close(bdev->sk_pair[1]);
	}
    }

    if (global_state.oxmem_info.sockfd > 0) {
	close(global_state.oxmem_info.sockfd);
    }

    printf("Cleanup complete\n");
}

/**
 * Parse size with unit (G/M/K)
 */
static size_t parse_size_with_unit(const char *size_str)
{
    if (!size_str)
	return 0;

    char *end;
    unsigned long long value = strtoull(size_str, &end, 10);
    if (end == size_str)
	return 0;

    size_t multiplier = 1;
    if (*end != '\0') {
	switch (*end) {
	case 'G':
	case 'g':
	    multiplier = 1024ULL * 1024ULL * 1024ULL;
	    break;
	case 'M':
	case 'm':
	    multiplier = 1024ULL * 1024ULL;
	    break;
	case 'K':
	case 'k':
	    multiplier = 1024ULL;
	    break;
	default:
	    return 0;
	}
    }

    return (size_t) (value * multiplier);
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
	   "  --size=SIZE     Total size with unit G/M/K (REQUIRED)\n\n"
	   "Optional options:\n"
	   "  --base=ADDR     Base address in hex (default: 0x0)\n"
	   "  --num=N         Number of NBD devices (default: 1, max: 16)\n"
	   "                  Size is divided equally among devices\n"
	   "  -h, --help      Show this help\n\n"
	   "Examples:\n"
	   "  %s --netdev=veth1 --mac=04:00:00:00:00:00 --size=1G\n"
	   "  %s --netdev=veth1 --mac=04:00:00:00:00:00 --size=8G --num=4\n"
	   "    (creates /dev/nbd0-3, each 2GB)\n", progname, progname);
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
    char *num_str = NULL;
    int num_devices = 1;
    uint32_t mac_values[6];
    struct sockaddr_ll saddr;
    struct ox_packet_struct send_ox, recv_ox;
    pthread_t nbd_threads[MAX_NBD_DEVICES];
    pthread_t nbd_do_it_threads[MAX_NBD_DEVICES];
    int i;
    size_t total_size, device_size;

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
	} else if (strncmp(argv[i], "--num=", 6) == 0) {
	    num_str = argv[i] + 6;
	} else if (strcmp(argv[i], "-h") == 0
		   || strcmp(argv[i], "--help") == 0) {
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
    // Parse num_devices
    if (num_str) {
	num_devices = atoi(num_str);
	if (num_devices < 1 || num_devices > MAX_NBD_DEVICES) {
	    fprintf(stderr, "Error: --num must be between 1 and %d\n",
		    MAX_NBD_DEVICES);
	    return 1;
	}
    }
    // Setup signal handlers
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    // Initialize global state
    memset(&global_state, 0, sizeof(global_state));
    global_state.num_devices = num_devices;

    // Initialize send_ox_list array
    memset(send_ox_list, 0, sizeof(send_ox_list));
    strncpy(global_state.oxmem_info.netdev, netdev,
	    sizeof(global_state.oxmem_info.netdev) - 1);
    global_state.oxmem_info.netdev_id = if_nametoindex(netdev);

    if (global_state.oxmem_info.netdev_id == 0) {
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
	global_state.oxmem_info.mac_addr +=
	    (uint64_t) mac_values[i] << (i * 8);
    }

    // Parse base and size
    global_state.oxmem_info.base = strtoull(base_str, NULL, 16);
    total_size = parse_size_with_unit(size_str);
    if (total_size == 0) {
	fprintf(stderr, "Error: Invalid size\n");
	return 1;
    }
    // Calculate per-device size
    device_size = total_size / num_devices;
    if (device_size == 0) {
	fprintf(stderr, "Error: Total size too small for %d devices\n",
		num_devices);
	return 1;
    }

    global_state.oxmem_info.st.st_size = global_state.oxmem_info.size =
	total_size;
    global_state.oxmem_info.connection_id = -1;

    printf("OmniXtend Block Device:\n");
    printf("  Network: %s\n", netdev);
    printf("  MAC: %s\n", mac_str);
    printf("  Total Size: %zu bytes (%zu MB)\n", total_size,
	   total_size / (1024 * 1024));
    printf("  Base: 0x%lx\n", global_state.oxmem_info.base);
    printf("  Number of devices: %d\n", num_devices);
    printf("  Size per device: %zu bytes (%zu MB)\n", device_size,
	   device_size / (1024 * 1024));

    // Create socket
    global_state.oxmem_info.sockfd =
	socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL));
    if (global_state.oxmem_info.sockfd < 0) {
	perror("Socket creation failed");
	return 1;
    }
    // Bind socket
    memset(&saddr, 0, sizeof(saddr));
    saddr.sll_family = AF_PACKET;
    saddr.sll_protocol = htons(ETH_P_ALL);
    saddr.sll_ifindex = global_state.oxmem_info.netdev_id;

    if (bind
	(global_state.oxmem_info.sockfd, (struct sockaddr *) &saddr,
	 sizeof(saddr)) < 0) {
	perror("Socket bind failed");
	close(global_state.oxmem_info.sockfd);
	return 1;
    }
    // Start receive thread
    pthread_t recv_tid;
    if (pthread_create(&recv_tid, NULL, recv_thread, NULL) != 0) {
	perror("Failed to create receive thread");
	close(global_state.oxmem_info.sockfd);
	return 1;
    }
    // Open connection (simplified - blocking)
    printf("Establishing OmniXtend connection...\n");

    global_state.oxmem_info.connection_id =
	make_open_connection_packet(global_state.oxmem_info.sockfd,
				    global_state.oxmem_info.netdev,
				    global_state.oxmem_info.mac_addr,
				    &send_ox);

    send_ox.tloe_hdr.chan = CHANNEL_A;
    send_ox.tloe_hdr.credit = DEFAULT_CREDIT;
    // Send on Channel A
    send_and_forget(&send_ox);

    // Send on Channel D
    send_ox.tloe_hdr.msg_type = NORMAL;
    send_ox.tloe_hdr.chan = CHANNEL_D;
    send_and_forget(&send_ox);

    printf("Connection established (ID: %d)\n",
	   global_state.oxmem_info.connection_id);

    // Initialize and start all NBD devices
    printf("\nSetting up %d NBD device(s)...\n", num_devices);
    for (i = 0; i < num_devices; i++) {
	struct oxmem_bdev *bdev = &global_state.bdevs[i];

	// Initialize device-specific fields
	bdev->device_index = i;
	bdev->device_size = device_size;
	bdev->device_base =
	    global_state.oxmem_info.base + (i * device_size);
	snprintf(bdev->device_path, sizeof(bdev->device_path),
		 "/dev/nbd%d", i);

	printf("  Device %d: %s (size: %zu bytes, base: 0x%lx)\n",
	       i, bdev->device_path, bdev->device_size, bdev->device_base);

	// Open NBD device
	bdev->nbd_fd = open(bdev->device_path, O_RDWR);
	if (bdev->nbd_fd < 0) {
	    fprintf(stderr, "Failed to open %s\n", bdev->device_path);
	    fprintf(stderr, "Hint: sudo modprobe nbd max_part=8\n");
	    cleanup_and_exit();
	    return 1;
	}
	// Create socketpair
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, bdev->sk_pair) < 0) {
	    perror("Failed to create socketpair");
	    close(bdev->nbd_fd);
	    cleanup_and_exit();
	    return 1;
	}
	// Configure NBD
	if (ioctl(bdev->nbd_fd, NBD_SET_SIZE, bdev->device_size) < 0) {
	    perror("NBD_SET_SIZE failed");
	    cleanup_and_exit();
	    return 1;
	}

	ioctl(bdev->nbd_fd, NBD_CLEAR_SOCK);
	ioctl(bdev->nbd_fd, NBD_SET_BLKSIZE, 4096);
	ioctl(bdev->nbd_fd, NBD_SET_TIMEOUT, 10);

	if (ioctl(bdev->nbd_fd, NBD_SET_SOCK, bdev->sk_pair[0]) < 0) {
	    perror("NBD_SET_SOCK failed");
	    cleanup_and_exit();
	    return 1;
	}
    }

    printf("\nAll block devices ready\n");
    printf("Press Ctrl+C to stop...\n");
    fflush(stdout);

    // Start NBD server threads (handle NBD protocol requests)
    for (i = 0; i < num_devices; i++) {
	if (pthread_create
	    (&nbd_threads[i], NULL, nbd_server_thread,
	     &global_state.bdevs[i]) != 0) {
	    fprintf(stderr,
		    "Failed to create NBD server thread for device %d\n",
		    i);
	    cleanup_and_exit();
	    return 1;
	}
    }

    usleep(100000);		// Small delay to let server threads start

    // Start NBD_DO_IT threads (one per device, each blocks until disconnect)
    for (i = 0; i < num_devices; i++) {
	if (pthread_create
	    (&nbd_do_it_threads[i], NULL, nbd_do_it_thread,
	     &global_state.bdevs[i]) != 0) {
	    fprintf(stderr,
		    "Failed to create NBD_DO_IT thread for device %d\n",
		    i);
	    cleanup_and_exit();
	    return 1;
	}
    }

    // Wait for all NBD_DO_IT threads (they block until disconnect)
    for (i = 0; i < num_devices; i++) {
	pthread_join(nbd_do_it_threads[i], NULL);
    }

    // Wait for all NBD server threads
    for (i = 0; i < num_devices; i++) {
	pthread_join(nbd_threads[i], NULL);
    }

    // Cleanup
    cleanup_and_exit();
    return 0;
}
