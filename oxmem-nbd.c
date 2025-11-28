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
#include "lib-queue.h"

/*
 * =============================================================================
 * Configuration Constants
 * =============================================================================
 */

/* Logging infrastructure */
#define LOG_INFO(fmt, ...)  printf("[INFO] " fmt "\n", ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) fprintf(stderr, "[ERROR] " fmt "\n", ##__VA_ARGS__)
#if 0
#define LOG_DEBUG(fmt, ...) printf("[DEBUG] %s:%d " fmt "\n", __func__, __LINE__, ##__VA_ARGS__)
#else
#define LOG_DEBUG(fmt, ...)
#endif

/* NBD configuration */
#define MAX_NBD_DEVICES     16
#define NBD_BLOCK_SIZE      4096

/* TileLink message configuration */
#define MAX_TL_MSG_LIST     1000
#define READ_WRITE_UNIT     1024    /* Max single request size in bytes */

/* Flow control configuration */
#define READ_COALESCE_COUNT 32 //32     /* Number of read requests to batch per packet */
#define MAX_INFLIGHT_READS  32 //32     /* Max concurrent read requests in flight */
#define MAX_INFLIGHT_WRITES 8 //8       /* Max concurrent write requests in flight */
#define MAX_RETRY_COUNT 5

/* Timeout configuration */
#define TIMEOUT_NS          10000000    /* 10ms in nanoseconds */
#define NS_PER_SEC          1000000000

/*
 * =============================================================================
 * Data Structures
 * =============================================================================
 */

/* Per-NBD device state */
struct oxmem_bdev {
    int nbd_fd;
    int sk_pair[2];
    size_t device_size;
    off_t device_base;
    int device_index;
    char device_path[32];
};

/* Global state shared across all NBD devices */
struct oxmem_global {
    struct oxmem_info_struct oxmem_info;
    unsigned int my_credit_in_use[5];
    unsigned int your_credit_remained[5];
    struct oxmem_bdev bdevs[MAX_NBD_DEVICES];
    int num_devices;
};

/* TileLink flit structure */
struct tl_flit {
    struct tl_msg_header_chan_AD hdr;
    uint64_t addr;
    uint64_t flits[READ_WRITE_UNIT / sizeof(uint64_t) + 2];
};

/* TileLink message status */
enum tl_status {
    TL_FREE,
    TL_IN_USE,
    TL_SENT,
    TL_RECEIVED
};

/* TileLink message list entry for tracking in-flight requests */
struct tl_msg_list_entry {
    struct tl_flit sent_tl;
    struct tl_flit recv_tl;
    int tl_status;
    sem_t sem;
};

/* Work item for post-processing received packets */
struct recv_work_item {
    char recv_buffer[BUFFER_SIZE];
    int recv_size;
    uint64_t id;
};

/*
 * =============================================================================
 * Global State
 * =============================================================================
 */

static struct tl_msg_list_entry tl_msg_list[MAX_TL_MSG_LIST];
static pthread_mutex_t tl_msg_list_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t credit_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t send_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t read_lock = PTHREAD_MUTEX_INITIALIZER;
static volatile sig_atomic_t shutdown_requested = 0;
static struct oxmem_global global_state;
static Queue q_send;
static sem_t send_sem;
static Queue q_recv_post;
static sem_t recv_post_sem;
static uint64_t recv_id = 0;

/*
 * =============================================================================
 * Function Declarations
 * =============================================================================
 */

/* Thread entry points */
void *nbd_server_thread(void *arg);
void *nbd_do_it_thread(void *arg);
void *recv_thread(void *arg);
void *post_handler_thread(void *arg);
void *send_thread(void *arg);

/* Cleanup and signal handling */
static void cleanup_and_exit(void);
static void signal_handler(int sig);

/* OmniXtend I/O operations */
static int oxmem_read_blocking(char *buf, size_t size, off_t offset,
                               struct oxmem_bdev *bdev);
static int oxmem_write_blocking(const char *buf, size_t size, off_t offset,
                                struct oxmem_bdev *bdev);

/* TileLink message management */
static void init_tl_msg_list(void);
static int get_tl_msg(void);
static void free_tl_msg(int i);

/* Credit management */
static int get_used_credit(struct ox_packet_struct *recv_ox, int channel);
static int get_ack_credit(int channel);
static void update_send_ox_credit(struct ox_packet_struct *send_ox);

/* Packet send helpers */
static int send_ox_packet(struct ox_packet_struct *send_ox);
static int send_ack(void);
static int send_open_connection(uint64_t my_mac, uint64_t dst_mac);
static void send_disconnection(void);

/* Utility functions */
static size_t round_down_to_power_of_2(size_t size, int *log2_size);
static void set_timeout(struct timespec *timeout, long ns);
static size_t parse_size_with_unit(const char *size_str);
static void show_help(const char *progname);

/*
 * =============================================================================
 * Utility Functions
 * =============================================================================
 */

/**
 * Round size down to nearest power of 2, returning both the rounded size
 * and the log2 value (useful for TileLink size field)
 */
static size_t
round_down_to_power_of_2(size_t size, int *log2_size)
{
    int i;
    for (i = 10; i >= 0; i--) {
        if (size >= (1U << i)) {
            if (log2_size)
                *log2_size = i;
            return 1U << i;
        }
    }
    if (log2_size)
        *log2_size = 0;
    return 1;
}

/**
 * Set a timeout relative to current time
 */
static void
set_timeout(struct timespec *timeout, long ns)
{
    clock_gettime(CLOCK_REALTIME, timeout);
    timeout->tv_nsec += ns;
    while (timeout->tv_nsec >= NS_PER_SEC) {
        timeout->tv_nsec -= NS_PER_SEC;
        timeout->tv_sec += 1;
    }
}

/*
 * =============================================================================
 * TileLink Message Management
 * =============================================================================
 */

static void
init_tl_msg_list(void)
{
    memset(tl_msg_list, 0, sizeof(tl_msg_list));
}

/**
 * Allocate a free TileLink message entry
 * Returns index on success, -1 if no free entries
 */
static int
get_tl_msg(void)
{
    int i;

    pthread_mutex_lock(&tl_msg_list_lock);
    for (i = 0; i < MAX_TL_MSG_LIST; i++) {
        if (tl_msg_list[i].tl_status == TL_FREE) {
            tl_msg_list[i].tl_status = TL_IN_USE;
            break;
        }
    }
    pthread_mutex_unlock(&tl_msg_list_lock);

    return (i < MAX_TL_MSG_LIST) ? i : -1;
}

/**
 * Free a TileLink message entry
 */
static void
free_tl_msg(int i)
{
    if (i < 0 || i >= MAX_TL_MSG_LIST)
        return;

    pthread_mutex_lock(&tl_msg_list_lock);
    if (tl_msg_list[i].tl_status != TL_FREE)
        memset(&tl_msg_list[i], 0, sizeof(struct tl_msg_list_entry));
    pthread_mutex_unlock(&tl_msg_list_lock);
}

/*
 * =============================================================================
 * Signal Handling and Cleanup
 * =============================================================================
 */

/**
 * Signal handler for graceful shutdown
 */
static void
signal_handler(int sig)
{
    int i;
    (void)sig;
    shutdown_requested = 1;

    cleanup_and_exit();

    for (i = 0; i < global_state.num_devices; i++) {
        if (global_state.bdevs[i].nbd_fd > 0)
            ioctl(global_state.bdevs[i].nbd_fd, NBD_DISCONNECT);
    }
    exit(0);
}

/*
 * =============================================================================
 * Credit Management
 * =============================================================================
 */

/**
 * Calculate credit used by received packet on a channel
 */
static int
get_used_credit(struct ox_packet_struct *recv_ox, int channel)
{
    int credit = 0;
    uint64_t tl_msg_mask = recv_ox->tl_msg_mask;

    for (int i = 0; i < 64 && tl_msg_mask; i++, tl_msg_mask >>= 1) {
        if (!(tl_msg_mask & 0x1))
            continue;

        credit++;

        struct tl_msg_header_chan_AD tl_msg_ack;
        uint64_t be64_temp = be64toh(recv_ox->flits[i]);
        memcpy(&tl_msg_ack, &be64_temp, sizeof(uint64_t));

        if (tl_msg_ack.chan == channel &&
            tl_msg_ack.opcode == D_ACCESSACKDATA_OPCODE) {
            credit += (tl_msg_ack.size >= 3) ? (1 << (tl_msg_ack.size - 3)) : 1;
        }
    }

    return credit;
}

/**
 * Get acknowledgement credit for a channel (rounds down to power of 2)
 */
static int
get_ack_credit(int channel)
{
    if (channel < CHANNEL_A || channel > CHANNEL_E)
        return 0;

    pthread_mutex_lock(&credit_lock);

    /* Find highest power of 2 <= credit_in_use */
    int credit = -1;
    for (int i = 31; i >= 0; i--) {
        if (global_state.my_credit_in_use[channel - 1] >= (1UL << i)) {
            credit = i;
            global_state.my_credit_in_use[channel - 1] -= (1UL << i);
            break;
        }
    }

    pthread_mutex_unlock(&credit_lock);

    return credit;
}

/**
 * Update outgoing packet with credit information
 */
static void
update_send_ox_credit(struct ox_packet_struct *send_ox)
{
    if (send_ox->tloe_hdr.chan != 0)
        return;

    int credit = get_ack_credit(CHANNEL_D);
    if (credit >= 0) {
        send_ox->tloe_hdr.credit = credit;
        send_ox->tloe_hdr.chan = CHANNEL_D;
    }
}

/*
 * =============================================================================
 * Packet Send Functions
 * =============================================================================
 */

/**
 * Queue a packet for sending via the send thread
 */
static int
send_ox_packet(struct ox_packet_struct *send_ox)
{
LOG_DEBUG("");
    struct ox_packet_struct *send_ox_buffer = malloc(sizeof(struct ox_packet_struct));
    if (!send_ox_buffer)
        return -ENOMEM;

    pthread_mutex_lock(&send_lock);
    memcpy(send_ox_buffer, send_ox, sizeof(struct ox_packet_struct));
    pthread_mutex_unlock(&send_lock);

    if (send_ox_buffer->tloe_hdr.msg_type != OPEN_CONN)
        send_ox_buffer->tloe_hdr.ack = 1;

    enqueue(&q_send, (uint64_t)send_ox_buffer);
    sem_post(&send_sem);

    return 0;
}

/**
 * Send a disconnection packet
 */
static void
send_disconnection(void)
{
    struct ox_packet_struct send_ox;

    setup_send_ox_eth_hdr(global_state.oxmem_info.connection_id, &send_ox);
    send_ox.tloe_hdr.msg_type = CLOSE_CONN;
    send_ox_packet(&send_ox);
    usleep(100000);
}

/**
 * Establish connection with OmniXtend device
 */
static int
send_open_connection(uint64_t my_mac, uint64_t dst_mac)
{
    struct ox_packet_struct send_ox;
    int connection_id;

    connection_id = add_new_connection(dst_mac, my_mac, 0x3FFFFF);
    if (connection_id < 0) {
        LOG_ERROR("connection creation failed");
        return -1;
    }

    global_state.oxmem_info.connection_id = connection_id;

    setup_send_ox_eth_hdr(connection_id, &send_ox);

    /* Send OPEN_CONN on Channel A */
    send_ox.tloe_hdr.chan = CHANNEL_A;
    send_ox.tloe_hdr.credit = DEFAULT_CREDIT;
    send_ox.tloe_hdr.msg_type = OPEN_CONN;
    send_ox_packet(&send_ox);

    /* Send NORMAL on Channel D */
    send_ox.tloe_hdr.msg_type = NORMAL;
    send_ox.tloe_hdr.chan = CHANNEL_D;
    send_ox_packet(&send_ox);

    return 0;
}

/**
 * Send an acknowledgement packet
 */
static int
send_ack(void)
{
    struct ox_packet_struct send_ox;

    setup_send_ox_eth_hdr(global_state.oxmem_info.connection_id, &send_ox);
    return send_ox_packet(&send_ox);
}


/**
 * Cleanup and exit - releases all resources
 */
static void
cleanup_and_exit(void)
{
    /* Close OmniXtend connection */
    if (global_state.oxmem_info.connection_id >= 0) {
        printf("\nClosing OmniXtend connection...\n");
        send_disconnection();
        delete_connection(global_state.oxmem_info.connection_id);
        global_state.oxmem_info.connection_id = -1;
    }

    /* Cleanup all NBD devices */
    for (int i = 0; i < global_state.num_devices; i++) {
        struct oxmem_bdev *bdev = &global_state.bdevs[i];

        if (bdev->nbd_fd > 0) {
            printf("Closing NBD device %s...\n", bdev->device_path);
            ioctl(bdev->nbd_fd, NBD_CLEAR_QUE);
            ioctl(bdev->nbd_fd, NBD_CLEAR_SOCK);
            close(bdev->nbd_fd);
        }

        if (bdev->sk_pair[0] > 0) {
            close(bdev->sk_pair[0]);
            close(bdev->sk_pair[1]);
        }
    }

    /* Close network socket */
    if (global_state.oxmem_info.sockfd > 0)
        close(global_state.oxmem_info.sockfd);

    printf("Cleanup complete\n");
}

/*
 * =============================================================================
 * OmniXtend I/O Operations
 * =============================================================================
 */

/**
 * Wait for a TileLink response with timeout and retransmit loop
 * Returns 0 on success (keeps retrying until success)
 */
static int
wait_for_response(int source, struct ox_packet_struct *send_ox,
                  off_t offset_for_log)
{
    struct timespec timeout;
    int ret;
    int retry_count = 0;

    set_timeout(&timeout, TIMEOUT_NS);
    ret = sem_timedwait(&tl_msg_list[source].sem, &timeout);

    if (ret == 0)
        return 0;

    /* Timeout - enter retransmit loop */
    while (ret != 0) {
        retry_count++;
        LOG_DEBUG("TIMEOUT source=%d offset=%lx - retransmitting (retry %d)",
                  source, offset_for_log, retry_count);

        send_ox->tl_msg_mask = 0x1;
        send_ox->flit_cnt = 2;
        send_ox->flits = tl_msg_list[source].sent_tl.flits;

        sem_destroy(&tl_msg_list[source].sem);
        sem_init(&tl_msg_list[source].sem, 0, 0);
        tl_msg_list[source].tl_status = TL_SENT;

        send_ox_packet(send_ox);

//        LOG_INFO("Retransmitted source=%d addr=%lx (retry %d), waiting again", source, tl_msg_list[source].sent_tl.addr, retry_count);

        set_timeout(&timeout, TIMEOUT_NS);
        ret = sem_timedwait(&tl_msg_list[source].sem, &timeout);

        if ( retry_count >= MAX_RETRY_COUNT ) return -1;;
    }

    return 0;
}

/**
 * Read from OmniXtend memory with flow control
 * Uses sliding window with coalesced requests for better performance
 */
static int
oxmem_read_blocking(char *buf, size_t size, off_t offset, struct oxmem_bdev *bdev)
{
    struct ox_packet_struct send_ox;
    size_t bytes_processed = 0;
    off_t absolute_offset = bdev->device_base + offset;
    int source_list[1024];
    size_t chunk_sizes[1024];
    size_t chunk_offsets[1024];
    int send_idx = 0;
    int wait_idx = 0;
    int coalesce_count = 0;

    /* Bounds check */
    if (offset + size > bdev->device_size) {
        LOG_ERROR("Read out of bounds (device %d)", bdev->device_index);
        return -EINVAL;
    }

    /* Connection check */
    if (global_state.oxmem_info.connection_id < 0) {
        LOG_ERROR("Not connected");
        return -ENXIO;
    }

//    LOG_INFO("READ offset=%lx size=%ld", offset, size);

    pthread_mutex_lock(&read_lock);

    setup_send_ox_eth_hdr(global_state.oxmem_info.connection_id, &send_ox);

    /* Read in chunks with sliding window flow control */
    while (bytes_processed < size || wait_idx < send_idx) {

        /* Send new requests while we have room in the window */
        while (bytes_processed < size &&
               (send_idx - wait_idx) < MAX_INFLIGHT_READS) {

            int log2_size;
            size_t chunk_size = size - bytes_processed;
            if (chunk_size > READ_WRITE_UNIT)
                chunk_size = READ_WRITE_UNIT;
            chunk_size = round_down_to_power_of_2(chunk_size, &log2_size);

            int source = get_tl_msg();
            if (source < 0) {
                LOG_ERROR("No free TL message slots");
                return -ENOMEM;
            }
            source_list[send_idx] = source;

            /* Setup TileLink GET message */
            struct tl_flit *tl = &tl_msg_list[source].sent_tl;
            tl->hdr.source = source;
            tl->hdr.size = log2_size;
            tl->hdr.opcode = A_GET_OPCODE;
            tl->hdr.chan = CHANNEL_A;
            tl->addr = absolute_offset + bytes_processed;

            uint64_t be64_temp;
            memcpy(&be64_temp, &tl->hdr, sizeof(uint64_t));
            tl->flits[0] = be64toh(be64_temp);
            tl->flits[1] = be64toh(tl->addr);

            sem_init(&tl_msg_list[source].sem, 0, 0);
            tl_msg_list[source].tl_status = TL_SENT;

            /* Coalesce multiple requests into single packet */
            if (send_ox.flits == NULL) {
                send_ox.flits = tl->flits;
            } else {
                send_ox.flits[coalesce_count * 2] = tl->flits[0];
                send_ox.flits[coalesce_count * 2 + 1] = tl->flits[1];
            }

            send_ox.tl_msg_mask |= 1UL << (coalesce_count * 2);
            send_ox.flit_cnt += 2;

            chunk_sizes[send_idx] = chunk_size;
            chunk_offsets[send_idx] = bytes_processed;

            bytes_processed += chunk_size;
            send_idx++;
            coalesce_count++;

            /* Flush coalesced requests when batch is full or done */
            if (coalesce_count >= READ_COALESCE_COUNT ||
                coalesce_count >= MAX_INFLIGHT_READS ||
                bytes_processed >= size) {
                send_ox_packet(&send_ox);
                send_ox.flits = NULL;
                send_ox.flit_cnt = 0;
                send_ox.tl_msg_mask = 0;
                coalesce_count = 0;
            }
        }

        /* Wait for oldest in-flight request */
        if (wait_idx < send_idx) {
            int source = source_list[wait_idx];

            if ( 0 == wait_for_response(source, &send_ox,
                              absolute_offset + chunk_offsets[wait_idx]) ) {
                ;
                /* Copy response data */
                memcpy(buf + chunk_offsets[wait_idx],
                       &tl_msg_list[source].recv_tl.flits[1],
                       chunk_sizes[wait_idx]);
            }

            sem_destroy(&tl_msg_list[source].sem);
            free_tl_msg(source);
            wait_idx++;
        }
    }

    pthread_mutex_unlock(&read_lock);
    return size;
}

/**
 * Wait for a write response with timeout and retransmit loop
 * Similar to wait_for_response but includes data flits in retransmit
 */
static int
wait_for_write_response(int source, struct ox_packet_struct *send_ox,
                        size_t chunk_size, off_t offset_for_log)
{
    struct timespec timeout;
    int ret;
    int retry_count = 0;

    set_timeout(&timeout, NS_PER_SEC);  /* 1 second for writes */
    ret = sem_timedwait(&tl_msg_list[source].sem, &timeout);

    if (ret == 0)
        return 0;

    /* Timeout - enter retransmit loop */
    while (ret != 0) {
        retry_count++;
        LOG_DEBUG("WRITE TIMEOUT source=%d offset=%lx - retransmitting (retry %d)",
                  source, offset_for_log, retry_count);

        send_ox->tl_msg_mask = 0x1;
        send_ox->flit_cnt = 2 + chunk_size / sizeof(uint64_t);
        send_ox->flits = tl_msg_list[source].sent_tl.flits;

        sem_destroy(&tl_msg_list[source].sem);
        sem_init(&tl_msg_list[source].sem, 0, 0);
        tl_msg_list[source].tl_status = TL_SENT;

        send_ox_packet(send_ox);

        LOG_DEBUG("Retransmitted source=%d (retry %d), waiting again", source, retry_count);

        set_timeout(&timeout, NS_PER_SEC);
        ret = sem_timedwait(&tl_msg_list[source].sem, &timeout);
    }

    return 0;
}

/**
 * Write to OmniXtend memory with flow control
 * Uses sliding window with MAX_INFLIGHT_WRITES concurrent requests
 */
static int
oxmem_write_blocking(const char *buf, size_t size, off_t offset,
                     struct oxmem_bdev *bdev)
{
    struct ox_packet_struct send_ox;
    size_t bytes_written = 0;
    off_t absolute_offset = bdev->device_base + offset;
    int source_list[1024];
    size_t chunk_sizes[1024];
    int send_idx = 0;
    int wait_idx = 0;

    /* Bounds check */
    if (offset + size > bdev->device_size) {
        LOG_ERROR("Write out of bounds (device %d)", bdev->device_index);
        return -EINVAL;
    }

//    LOG_INFO("WRITE offset=%lx size=%ld", offset, size);

    setup_send_ox_eth_hdr(global_state.oxmem_info.connection_id, &send_ox);

    /* Write in chunks with sliding window flow control */
    while (bytes_written < size || wait_idx < send_idx) {

        /* Send new requests while we have room in the window */
        while (bytes_written < size &&
               (send_idx - wait_idx) < MAX_INFLIGHT_WRITES) {

            int log2_size;
            size_t chunk_size = size - bytes_written;
            if (chunk_size > READ_WRITE_UNIT)
                chunk_size = READ_WRITE_UNIT;
            chunk_size = round_down_to_power_of_2(chunk_size, &log2_size);

            int source = get_tl_msg();
            if (source < 0) {
                LOG_ERROR("No free TL message slots");
                return -ENOMEM;
            }
            source_list[send_idx] = source;

            /* Setup TileLink PUTFULLDATA message */
            struct tl_flit *tl = &tl_msg_list[source].sent_tl;
            tl->hdr.source = source;
            tl->hdr.size = log2_size;
            tl->hdr.opcode = A_PUTFULLDATA_OPCODE;
            tl->hdr.chan = CHANNEL_A;

            uint64_t be64_temp;
            memcpy(&be64_temp, &tl->hdr, sizeof(uint64_t));
            tl->flits[0] = be64toh(be64_temp);

            be64_temp = absolute_offset + bytes_written;
            tl->flits[1] = be64toh(be64_temp);

            /* Copy data to flits */
            memcpy(&tl->flits[2], buf + bytes_written, chunk_size);

            send_ox.tl_msg_mask = 0x1;
            send_ox.flit_cnt = 2 + chunk_size / sizeof(uint64_t);
            send_ox.flits = tl->flits;
            tl_msg_list[source].tl_status = TL_SENT;

            sem_init(&tl_msg_list[source].sem, 0, 0);
            send_ox_packet(&send_ox);

            chunk_sizes[send_idx] = chunk_size;
            bytes_written += chunk_size;
            send_idx++;
        }

        /* Wait for oldest in-flight request */
        if (wait_idx < send_idx) {
            int source = source_list[wait_idx];

            wait_for_write_response(source, &send_ox, chunk_sizes[wait_idx],
                                    absolute_offset + bytes_written);

            sem_destroy(&tl_msg_list[source].sem);
            free_tl_msg(source);
            wait_idx++;
        }
    }

    return size;
}

/*
 * =============================================================================
 * NBD Server Threads
 * =============================================================================
 */

/**
 * NBD_DO_IT thread - blocks in kernel until device is disconnected
 */
void *
nbd_do_it_thread(void *arg)
{
    struct oxmem_bdev *bdev = (struct oxmem_bdev *)arg;

    printf("NBD_DO_IT thread started for %s\n", bdev->device_path);

    if (ioctl(bdev->nbd_fd, NBD_DO_IT) < 0 && !shutdown_requested) {
        LOG_ERROR("NBD_DO_IT failed for %s: %s",
                  bdev->device_path, strerror(errno));
    }

    printf("NBD_DO_IT thread exiting for %s\n", bdev->device_path);
    return NULL;
}

/**
 * NBD server thread - handles NBD protocol requests
 */
void *
nbd_server_thread(void *arg)
{
    struct oxmem_bdev *bdev = (struct oxmem_bdev *)arg;
    struct nbd_request request;
    struct nbd_reply reply;
    char *buffer;
    ssize_t bytes_read;

LOG_DEBUG("");
    buffer = malloc(1024 * 1024);  /* 1MB buffer */
    if (!buffer) {
        LOG_ERROR("Failed to allocate buffer for device %d", bdev->device_index);
        return NULL;
    }

    printf("NBD server thread started for %s\n", bdev->device_path);

    while (!shutdown_requested) {
        bytes_read = read(bdev->sk_pair[1], &request, sizeof(request));

        if (bytes_read == 0) {
            printf("NBD connection closed for %s\n", bdev->device_path);
            break;
        }

        if (bytes_read < 0) {
            if (errno == EINTR)
                continue;
            LOG_ERROR("Failed to read NBD request: %s", strerror(errno));
            break;
        }

        if (bytes_read != sizeof(request)) {
            LOG_ERROR("Incomplete NBD request");
            continue;
        }

        /* Parse request */
        uint32_t type = ntohl(request.type);
        uint64_t offset = be64toh(*(uint64_t *)&request.from);
        uint32_t len = ntohl(request.len);

        /* Prepare reply */
        memset(&reply, 0, sizeof(reply));
        reply.magic = htonl(NBD_REPLY_MAGIC);
        memcpy(reply.handle, request.handle, sizeof(reply.handle));
        reply.error = 0;

        switch (type) {
        case NBD_CMD_READ: {
            int result = oxmem_read_blocking(buffer, len, offset, bdev);
            if (result > 0) {
                write(bdev->sk_pair[1], &reply, sizeof(reply));
                write(bdev->sk_pair[1], buffer, len);
            } else {
                reply.error = htonl(EIO);
                write(bdev->sk_pair[1], &reply, sizeof(reply));
            }
            break;
        }

        case NBD_CMD_WRITE: {
            /* Read write data from socket */
            ssize_t total_bytes = 0;
            while (total_bytes < len) {
                bytes_read = read(bdev->sk_pair[1], buffer + total_bytes,
                                  len - total_bytes);
                if (bytes_read < 0) {
                    reply.error = htonl(EIO);
                    write(bdev->sk_pair[1], &reply, sizeof(reply));
                    break;
                }
                total_bytes += bytes_read;
            }

            if (total_bytes < len)
                break;

            int result = oxmem_write_blocking(buffer, len, offset, bdev);
            reply.error = (result > 0) ? 0 : htonl(EIO);
            write(bdev->sk_pair[1], &reply, sizeof(reply));
            break;
        }

        case NBD_CMD_DISC:
            printf("NBD disconnect requested for %s\n", bdev->device_path);
            goto cleanup;

        case NBD_CMD_FLUSH:
            /* No-op for network device */
            write(bdev->sk_pair[1], &reply, sizeof(reply));
            break;

        default:
            LOG_INFO("Unknown NBD command: %u", type);
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

/*
 * =============================================================================
 * Network I/O Threads
 * =============================================================================
 */

/**
 * Send thread - serializes packet transmission
 */
void *
send_thread(void *arg)
{
    char send_buffer[BUFFER_SIZE];
    int send_size;
    struct ox_packet_struct *send_ox = NULL;
    int running = 1;

    (void)arg;

    sem_init(&send_sem, 0, 0);

    while (running) {
        send_ox = (struct ox_packet_struct *)dequeue(&q_send);

        if (send_ox == NULL) {
            sem_wait(&send_sem);
            continue;
        }

        /* Set sequence number and credit */
        set_seq_num_to_ox_packet(global_state.oxmem_info.connection_id, send_ox);
        update_send_ox_credit(send_ox);

        /* Convert to wire format and send */
        ox_struct_to_packet(send_ox, send_buffer, &send_size);

        if (send(global_state.oxmem_info.sockfd, send_buffer, send_size, 0) < 0)
            LOG_ERROR("send failed: %s", strerror(errno));

        if (send_ox->tloe_hdr.msg_type == CLOSE_CONN) {
            LOG_DEBUG("CLOSE_CONN packet sent");
            running = 0;
        }

        free(send_ox);
    }

    sem_destroy(&send_sem);
    return NULL;
}

/**
 * Post-handler thread - processes received packets
 * Heavy processing separated from recv_thread for better performance
 */
void *
post_handler_thread(void *arg)
{
    struct recv_work_item *work_item;
    struct ox_packet_struct recv_ox;

    (void)arg;

    printf("Post-handler thread started\n");

    while (1) {
        work_item = (struct recv_work_item *)dequeue(&q_recv_post);
        if (work_item == NULL) {
            if (shutdown_requested)
                break;
            sem_wait(&recv_post_sem);
            continue;
        }

        /* Parse received packet */
        packet_to_ox_struct(work_item->recv_buffer, work_item->recv_size, &recv_ox);

        /* Update expected sequence number */
        update_seq_num_expected(global_state.oxmem_info.connection_id, &recv_ox);

        /* Handle ACK-only packets */
        if (recv_ox.tloe_hdr.msg_type == ACK_ONLY) {
            free(work_item);
            continue;
        }

        /* Handle CLOSE_CONN */
        if (recv_ox.tloe_hdr.msg_type == CLOSE_CONN) {
            LOG_DEBUG("CLOSE_CONN packet received");
            free(work_item);
            break;
        }

        /* Send ACK if send queue is empty */
        if (isEmpty(&q_send))
            send_ack();

        /* Update remote credit */
        if (recv_ox.tloe_hdr.chan > 0) {
            pthread_mutex_lock(&credit_lock);
            global_state.your_credit_remained[recv_ox.tloe_hdr.chan - 1] +=
                1 << recv_ox.tloe_hdr.credit;
            pthread_mutex_unlock(&credit_lock);
        }

        /* Process TileLink messages */
        if (recv_ox.tl_msg_mask != 0) {
            uint64_t mask = recv_ox.tl_msg_mask;

            pthread_mutex_lock(&credit_lock);
            global_state.my_credit_in_use[CHANNEL_D - 1] +=
                get_used_credit(&recv_ox, CHANNEL_D);
            pthread_mutex_unlock(&credit_lock);

            /* Match responses to pending requests */
            for (int i = 0; i < 64 && mask; i++, mask >>= 1) {
                if (!(mask & 0x1))
                    continue;

                struct tl_msg_header_chan_AD tl_msg_header;
                uint64_t be64_temp = be64toh(recv_ox.flits[i]);
                memcpy(&tl_msg_header, &be64_temp, sizeof(uint64_t));

                int source = tl_msg_header.source;

                if (source >= MAX_TL_MSG_LIST) {
                    LOG_DEBUG("Invalid source=%d in response", source);
                    continue;
                }

                if (tl_msg_list[source].tl_status != TL_SENT)
                    continue;

                /* Copy TileLink header */
                memcpy(&tl_msg_list[source].recv_tl.hdr, &tl_msg_header,
                       sizeof(struct tl_msg_header_chan_AD));

                /* Copy data for ACCESSACKDATA responses */
                if (tl_msg_header.chan == CHANNEL_D &&
                    tl_msg_header.opcode == D_ACCESSACKDATA_OPCODE) {
                    memcpy(&tl_msg_list[source].recv_tl.flits[0],
                           &recv_ox.flits[i],
                           (1 << tl_msg_header.size) + sizeof(uint64_t));
                }

                /* Signal waiting thread */
                tl_msg_list[source].tl_status = TL_RECEIVED;
                sem_post(&tl_msg_list[source].sem);
            }
        }

        free(work_item);
    }

    printf("Post-handler thread exiting\n");
    return NULL;
}

/**
 * Receive thread - fast path packet reception
 * Minimal processing, immediately hands off to post_handler_thread
 */
void *
recv_thread(void *arg)
{
    char recv_buffer[BUFFER_SIZE];
    int recv_size;

    (void)arg;

    printf("Receive thread started\n");

    while (1) {
        recv_size = recv(global_state.oxmem_info.sockfd, recv_buffer,
                         BUFFER_SIZE, 0);

        if (recv_size < 0) {
            if (shutdown_requested)
                break;
            LOG_ERROR("recv failed: %s", strerror(errno));
            continue;
        }

        /* Fast filter: check ethertype */
        struct ethhdr *eth_hdr = (struct ethhdr *)recv_buffer;
        if (eth_hdr->h_proto != OX_ETHERTYPE)
            continue;

LOG_DEBUG("");
        /* Allocate and enqueue work item */
        struct recv_work_item *work_item = malloc(sizeof(struct recv_work_item));
        if (!work_item) {
            LOG_ERROR("Failed to allocate recv work item");
            continue;
        }

        work_item->id = recv_id++;
        memcpy(work_item->recv_buffer, recv_buffer, recv_size);
        work_item->recv_size = recv_size;

        enqueue(&q_recv_post, (uint64_t)work_item);
        sem_post(&recv_post_sem);
    }

    printf("Receive thread exiting\n");
    return NULL;
}

/*
 * =============================================================================
 * Command Line Parsing and Help
 * =============================================================================
 */

/**
 * Parse size string with unit suffix (G/M/K)
 */
static size_t
parse_size_with_unit(const char *size_str)
{
    if (!size_str)
        return 0;

    char *end;
    unsigned long long value = strtoull(size_str, &end, 10);
    if (end == size_str)
        return 0;

    size_t multiplier = 1;
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
    case '\0':
        break;
    default:
        return 0;
    }

    return (size_t)(value * multiplier);
}

/**
 * Display usage information
 */
static void
show_help(const char *progname)
{
    printf("Usage: %s [options]\n\n", progname);
    printf("OmniXtend Memory Block Device Driver\n\n");
    printf("Required options:\n"
           "  --netdev=DEV    Network interface\n"
           "  --mac=MAC       MAC address of OmniXtend endpoint\n"
           "  --size=SIZE     Total size with unit G/M/K\n\n"
           "Optional options:\n"
           "  --base=ADDR     Base address in hex (default: 0x0)\n"
           "  --num=N         Number of NBD devices (default: 1, max: %d)\n"
           "  -h, --help      Show this help\n\n"
           "Examples:\n"
           "  %s --netdev=eth0 --mac=04:00:00:00:00:00 --size=1G\n"
           "  %s --netdev=eth0 --mac=04:00:00:00:00:00 --size=8G --num=4\n",
           MAX_NBD_DEVICES, progname, progname);
}

/*
 * =============================================================================
 * Main Entry Point
 * =============================================================================
 */

int
main(int argc, char *argv[])
{
    char *netdev = NULL;
    char *mac_str = NULL;
    char *size_str = NULL;
    char *base_str = "0x0";
    char *num_str = NULL;
    int num_devices = 1;
    uint32_t mac_values[6];
    struct sockaddr_ll saddr;
    pthread_t nbd_threads[MAX_NBD_DEVICES];
    pthread_t nbd_do_it_threads[MAX_NBD_DEVICES];
    pthread_t send_tid, recv_tid, post_handler_tid;
    size_t total_size, device_size;
    uint64_t my_mac;

    /* Parse command line arguments */
    for (int i = 1; i < argc; i++) {
        if (strncmp(argv[i], "--netdev=", 9) == 0)
            netdev = argv[i] + 9;
        else if (strncmp(argv[i], "--mac=", 6) == 0)
            mac_str = argv[i] + 6;
        else if (strncmp(argv[i], "--size=", 7) == 0)
            size_str = argv[i] + 7;
        else if (strncmp(argv[i], "--base=", 7) == 0)
            base_str = argv[i] + 7;
        else if (strncmp(argv[i], "--num=", 6) == 0)
            num_str = argv[i] + 6;
        else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            show_help(argv[0]);
            return 0;
        }
    }

    /* Validate required arguments */
    if (!netdev || !mac_str || !size_str) {
        LOG_ERROR("Missing required arguments");
        show_help(argv[0]);
        return 1;
    }

    if (num_str) {
        num_devices = atoi(num_str);
        if (num_devices < 1 || num_devices > MAX_NBD_DEVICES) {
            LOG_ERROR("--num must be between 1 and %d", MAX_NBD_DEVICES);
            return 1;
        }
    }

    /* Setup signal handlers */
    struct sigaction sa = {0};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* Initialize global state */
    memset(&global_state, 0, sizeof(global_state));
    global_state.num_devices = num_devices;
    global_state.oxmem_info.connection_id = -1;

    strncpy(global_state.oxmem_info.netdev, netdev,
            sizeof(global_state.oxmem_info.netdev) - 1);
    global_state.oxmem_info.netdev_id = if_nametoindex(netdev);

    if (global_state.oxmem_info.netdev_id == 0) {
        LOG_ERROR("Invalid network device: %s", netdev);
        return 1;
    }

    /* Parse MAC address */
    if (sscanf(mac_str, "%2x:%2x:%2x:%2x:%2x:%2x",
               &mac_values[0], &mac_values[1], &mac_values[2],
               &mac_values[3], &mac_values[4], &mac_values[5]) != 6) {
        LOG_ERROR("Invalid MAC address format");
        return 1;
    }

    for (int i = 0; i < 6; i++)
        global_state.oxmem_info.mac_addr += (uint64_t)mac_values[i] << (i * 8);

    /* Parse base address and size */
    global_state.oxmem_info.base = strtoull(base_str, NULL, 16);
    total_size = parse_size_with_unit(size_str);
    if (total_size == 0) {
        LOG_ERROR("Invalid size");
        return 1;
    }

    device_size = total_size / num_devices;
    if (device_size == 0) {
        LOG_ERROR("Total size too small for %d devices", num_devices);
        return 1;
    }

    global_state.oxmem_info.st.st_size = global_state.oxmem_info.size = total_size;

    printf("OmniXtend Block Device:\n");
    printf("  Network: %s\n", netdev);
    printf("  MAC: %s\n", mac_str);
    printf("  Total Size: %zu bytes (%zu MB)\n", total_size, total_size / (1024 * 1024));
    printf("  Base: 0x%lx\n", global_state.oxmem_info.base);
    printf("  Devices: %d x %zu MB\n", num_devices, device_size / (1024 * 1024));

    /* Initialize TileLink message list */
    init_tl_msg_list();

    /* Create raw socket */
    global_state.oxmem_info.sockfd = socket(AF_PACKET, SOCK_RAW, htons(OX_ETHERTYPE));
    if (global_state.oxmem_info.sockfd < 0) {
        LOG_ERROR("Socket creation failed: %s", strerror(errno));
        return 1;
    }

    /* Bind socket to interface */
    memset(&saddr, 0, sizeof(saddr));
    saddr.sll_family = AF_PACKET;
    saddr.sll_protocol = htons(ETH_P_ALL);
    saddr.sll_ifindex = global_state.oxmem_info.netdev_id;

    if (bind(global_state.oxmem_info.sockfd, (struct sockaddr *)&saddr,
             sizeof(saddr)) < 0) {
        LOG_ERROR("Socket bind failed: %s", strerror(errno));
        close(global_state.oxmem_info.sockfd);
        return 1;
    }

    /* Increase socket buffer sizes for better performance */
    int sockbuf_size = 4 * 1024 * 1024;
    setsockopt(global_state.oxmem_info.sockfd, SOL_SOCKET, SO_RCVBUF,
               &sockbuf_size, sizeof(sockbuf_size));
    setsockopt(global_state.oxmem_info.sockfd, SOL_SOCKET, SO_SNDBUF,
               &sockbuf_size, sizeof(sockbuf_size));

    /* Initialize queues and start worker threads */
    initQueue(&q_send);
    initQueue(&q_recv_post);
    sem_init(&recv_post_sem, 0, 0);

    if (pthread_create(&send_tid, NULL, send_thread, NULL) != 0 ||
        pthread_create(&post_handler_tid, NULL, post_handler_thread, NULL) != 0 ||
        pthread_create(&recv_tid, NULL, recv_thread, NULL) != 0) {
        LOG_ERROR("Failed to create worker threads");
        close(global_state.oxmem_info.sockfd);
        return 1;
    }

    /* Establish OmniXtend connection */
    printf("Establishing OmniXtend connection...\n");
    my_mac = get_mac_addr_from_devname(global_state.oxmem_info.sockfd, netdev);

    if (send_open_connection(my_mac, global_state.oxmem_info.mac_addr) < 0) {
        LOG_ERROR("Connection failed");
        return 1;
    }

    printf("Connection established (ID: %d)\n", global_state.oxmem_info.connection_id);

    /* Initialize NBD devices */
    printf("\nSetting up %d NBD device(s)...\n", num_devices);

    for (int i = 0; i < num_devices; i++) {
        struct oxmem_bdev *bdev = &global_state.bdevs[i];

        bdev->device_index = i;
        bdev->device_size = device_size;
        bdev->device_base = global_state.oxmem_info.base + (i * device_size);
        snprintf(bdev->device_path, sizeof(bdev->device_path), "/dev/nbd%d", i);

        printf("  %s: %zu MB at 0x%lx\n",
               bdev->device_path, bdev->device_size / (1024 * 1024), bdev->device_base);

        bdev->nbd_fd = open(bdev->device_path, O_RDWR);
        if (bdev->nbd_fd < 0) {
            LOG_ERROR("Failed to open %s (try: sudo modprobe nbd max_part=8)",
                      bdev->device_path);
            cleanup_and_exit();
            return 1;
        }

        if (socketpair(AF_UNIX, SOCK_STREAM, 0, bdev->sk_pair) < 0) {
            LOG_ERROR("Failed to create socketpair: %s", strerror(errno));
            cleanup_and_exit();
            return 1;
        }

        /* Configure NBD device */
        ioctl(bdev->nbd_fd, NBD_CLEAR_SOCK);
        if (ioctl(bdev->nbd_fd, NBD_SET_SIZE, bdev->device_size) < 0 ||
            ioctl(bdev->nbd_fd, NBD_SET_BLKSIZE, NBD_BLOCK_SIZE) < 0 ||
            ioctl(bdev->nbd_fd, NBD_SET_SOCK, bdev->sk_pair[0]) < 0) {
            LOG_ERROR("NBD configuration failed: %s", strerror(errno));
            cleanup_and_exit();
            return 1;
        }
        ioctl(bdev->nbd_fd, NBD_SET_TIMEOUT, 10);
    }

    printf("\nAll block devices ready. Press Ctrl+C to stop.\n");
    fflush(stdout);

    /* Start NBD server threads */
    for (int i = 0; i < num_devices; i++) {
        if (pthread_create(&nbd_threads[i], NULL, nbd_server_thread,
                           &global_state.bdevs[i]) != 0) {
            LOG_ERROR("Failed to create NBD server thread for device %d", i);
            cleanup_and_exit();
            return 1;
        }
    }

    usleep(100000);  /* Let server threads start */

    /* Start NBD_DO_IT threads */
    for (int i = 0; i < num_devices; i++) {
        if (pthread_create(&nbd_do_it_threads[i], NULL, nbd_do_it_thread,
                           &global_state.bdevs[i]) != 0) {
            LOG_ERROR("Failed to create NBD_DO_IT thread for device %d", i);
            cleanup_and_exit();
            return 1;
        }
    }

    /* Wait for threads to complete */
    for (int i = 0; i < num_devices; i++) {
        pthread_join(nbd_do_it_threads[i], NULL);
        pthread_join(nbd_threads[i], NULL);
    }

    cleanup_and_exit();
    return 0;
}
