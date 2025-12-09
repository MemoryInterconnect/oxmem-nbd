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
 * Statistics Tracking
 * =============================================================================
 */

/* Statistics structure for tracking I/O operations */
struct io_stats {
    uint64_t read_count;
    uint64_t read_bytes;
    uint64_t write_count;
    uint64_t write_bytes;
    pthread_mutex_t lock;
};

static struct io_stats global_stats = {
    .read_count = 0,
    .read_bytes = 0,
    .write_count = 0,
    .write_bytes = 0,
    .lock = PTHREAD_MUTEX_INITIALIZER
};

#define STATS_UPDATE_INTERVAL_DEFAULT 2
#define STATS_FILE_PATH "/tmp/memory_node.json"

static int stats_update_interval = STATS_UPDATE_INTERVAL_DEFAULT;

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
#define MAX_TL_MSG_LIST     4096
#define READ_WRITE_UNIT     1024    /* Max single request size in bytes */

/* Flow control configuration */
#define READ_COALESCE_COUNT 32 //32     /* Number of read requests to batch per packet */
#define MAX_INFLIGHT_READS  96 //96     /* Max concurrent read requests in flight */
#define MAX_INFLIGHT_WRITES 32 //32          /* Max concurrent write requests in flight */
#define MAX_RETRY_COUNT 5

/* Timeout configuration */
#define TIMEOUT_NS          100000000     /* 10ms in nanoseconds */
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
    uint64_t tl_addr;
    char * host_buf;
    size_t size;
    uint64_t * flits;
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

struct ox_packet_struct * alloc_ox_packet_struct(void);

/* Thread entry points */
void *nbd_server_thread(void *arg);
void *nbd_do_it_thread(void *arg);
void *recv_thread(void *arg);
void *post_handler_thread(void *arg);
void *send_thread(void *arg);
void *stats_thread(void *arg);

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
    for (i = 13; i >= 0; i--) {
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

int last_tl_source = 0;

static int
get_tl_msg(void)
{
    int i;
    int source;

    pthread_mutex_lock(&tl_msg_list_lock);
    for (i = 0; i < MAX_TL_MSG_LIST; i++) {
        source = i+last_tl_source+1;
        source %= MAX_TL_MSG_LIST;
        if (tl_msg_list[source].tl_status == TL_FREE) {
            tl_msg_list[source].tl_status = TL_IN_USE;
            break;
        }
    }
    if ( i < MAX_TL_MSG_LIST ) {
        last_tl_source = source;
    }
    pthread_mutex_unlock(&tl_msg_list_lock);

    return (i < MAX_TL_MSG_LIST) ? source : -1;
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
    if (!send_ox)
        return -ENOMEM;

    if (send_ox->tloe_hdr.msg_type != OPEN_CONN)
        send_ox->tloe_hdr.ack = 1;

    enqueue(&q_send, (uint64_t)send_ox);
    sem_post(&send_sem);

    return 0;
}

/**
 * Send a disconnection packet
 */
static void
send_disconnection(void)
{
    struct ox_packet_struct * send_ox;

    send_ox = alloc_ox_packet_struct();
    send_ox->tloe_hdr.msg_type = CLOSE_CONN;
    send_ox_packet(send_ox);
    usleep(100000);
}

/**
 * Establish connection with OmniXtend device
 */
static int
send_open_connection(uint64_t my_mac, uint64_t dst_mac)
{
    struct ox_packet_struct * send_ox;
    int connection_id;

    connection_id = add_new_connection(dst_mac, my_mac, 0x3FFFFF);
    if (connection_id < 0) {
        LOG_ERROR("connection creation failed");
        return -1;
    }

    global_state.oxmem_info.connection_id = connection_id;

    /* Send OPEN_CONN on Channel A */
    send_ox = alloc_ox_packet_struct();
    send_ox->tloe_hdr.chan = CHANNEL_A;
    send_ox->tloe_hdr.credit = DEFAULT_CREDIT;
    send_ox->tloe_hdr.msg_type = OPEN_CONN;
    send_ox_packet(send_ox);


    /* Send NORMAL on Channel D */
    send_ox = alloc_ox_packet_struct();
    send_ox->tloe_hdr.chan = CHANNEL_D;
    send_ox->tloe_hdr.credit = DEFAULT_CREDIT;
    send_ox->tloe_hdr.msg_type = NORMAL;
    send_ox_packet(send_ox);

    return 0;
}

/**
 * Send an acknowledgement packet
 */
static int
send_ack(void)
{
    struct ox_packet_struct * send_ox;

    send_ox = alloc_ox_packet_struct();

    return send_ox_packet(send_ox);
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

    /* Drain and free any remaining packets in queues */
    struct ox_packet_struct *ox;
    while ((ox = (struct ox_packet_struct *)dequeue(&q_send)) != NULL)
        free(ox);
    while ((ox = (struct ox_packet_struct *)dequeue(&q_recv_post)) != NULL)
        free(ox);

    /* Cleanup semaphores */
    sem_destroy(&recv_post_sem);

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

uint64_t retransmit_count = 0;

static int
wait_for_response(int source)
{
    struct timespec timeout;
    int ret;
    int retry_count = 0;
    struct ox_packet_struct *send_ox;
    uint64_t be64_temp;
    struct tl_flit *tl = &tl_msg_list[source].sent_tl;

    set_timeout(&timeout, TIMEOUT_NS);
    ret = sem_timedwait(&tl_msg_list[source].sem, &timeout);

 //   if (ret == 0)
//        return 0;

    /* Timeout - enter retransmit loop */
    while (ret != 0) {
        retry_count++;
        LOG_DEBUG("READ TIMEOUT source=%d offset=%lx - retransmitting (retry %d)",
                  source, tl->tl_addr, retry_count);

        send_ox = alloc_ox_packet_struct();
        send_ox->tl_msg_mask = 0x1;
        send_ox->flit_cnt = 2;

        memcpy(&be64_temp, &tl->hdr, sizeof(uint64_t));
        send_ox->flits[0] = be64toh(be64_temp);
        send_ox->flits[1] = be64toh(tl->tl_addr);

        sem_destroy(&tl_msg_list[source].sem);
        sem_init(&tl_msg_list[source].sem, 0, 0);
        tl_msg_list[source].tl_status = TL_SENT;

        send_ox_packet(send_ox);

        retransmit_count ++;
        LOG_INFO("READ Retransmitted source=%d addr=%lx (retry %d) retransmit total =%ld", source, tl_msg_list[source].sent_tl.tl_addr, retry_count, retransmit_count);

        set_timeout(&timeout, TIMEOUT_NS*5);
        ret = sem_timedwait(&tl_msg_list[source].sem, &timeout);

        if ( retry_count >= MAX_RETRY_COUNT ) return -1;;
    }

    return 0;
}

struct ox_packet_struct * alloc_ox_packet_struct(void)
{
    struct ox_packet_struct * ox;

    ox = malloc(sizeof(struct ox_packet_struct));

    if ( ox == NULL ) {
        LOG_ERROR("malloc error!");
        return NULL;
    }

    memset(ox, 0, sizeof(struct ox_packet_struct));
    ox->flits = (void*)(ox->packet_buffer) + sizeof(struct eth_header) + sizeof(struct tloe_header);

    setup_send_ox_eth_hdr(global_state.oxmem_info.connection_id, ox);

    return ox;
}

/**
 * Read from OmniXtend memory with flow control
 * Uses sliding window with coalesced requests for better performance
 */
static int
oxmem_read_blocking(char *buf, size_t size, off_t offset, struct oxmem_bdev *bdev)
{
    struct ox_packet_struct * send_ox;
    size_t bytes_processed = 0;
    off_t absolute_offset = bdev->device_base + offset;
    int source_list[MAX_TL_MSG_LIST];
    int send_idx = 0;
    int wait_idx = 0;
    int coalesce_count = 0;
    uint64_t be64_temp;
    struct tl_flit *tl = NULL;

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

    LOG_DEBUG("READ buf=%p offset=%lx size=%ld", buf, offset, size);

    pthread_mutex_lock(&read_lock);

    send_ox = alloc_ox_packet_struct();

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
                free(send_ox);
                send_ox = NULL;
                size = -ENOMEM;
                goto out;
            }
            source_list[send_idx] = source;

            /* Setup TileLink GET message */
            tl = &tl_msg_list[source].sent_tl;
            tl->hdr.source = source;
            tl->hdr.size = log2_size;
            tl->hdr.opcode = A_GET_OPCODE;
            tl->hdr.chan = CHANNEL_A;
            tl->tl_addr = absolute_offset + bytes_processed; //target buffer address

            tl->size = chunk_size;
            tl->host_buf = buf + bytes_processed;

            /* Coalesce multiple requests into single packet */
            memcpy(&be64_temp, &tl->hdr, sizeof(uint64_t));

            send_ox->flits[coalesce_count * 2] = be64toh(be64_temp);
            send_ox->flits[coalesce_count * 2 + 1] = be64toh(tl->tl_addr);

            sem_init(&tl_msg_list[source].sem, 0, 0);
            tl_msg_list[source].tl_status = TL_SENT;

            send_ox->tl_msg_mask |= 1UL << (coalesce_count * 2);
            send_ox->flit_cnt += 2;

            bytes_processed += chunk_size;
            send_idx++;
            coalesce_count++;

            /* Flush coalesced requests when batch is full or done */
            if (coalesce_count >= READ_COALESCE_COUNT ||
                coalesce_count >= MAX_INFLIGHT_READS ||
                bytes_processed >= size) {

                //hand over the send_ox to send_thread
                send_ox_packet(send_ox);

                if ( bytes_processed < size ) {
                    //it needs new send_ox now
                    send_ox = alloc_ox_packet_struct();
                } else {
                    send_ox = NULL;
                }

                coalesce_count = 0;
            }
        }

        /* Wait for oldest in-flight request */
        if (wait_idx < send_idx) {
            int source = source_list[wait_idx];

            if ( 0 != wait_for_response(source) ) {
                LOG_ERROR("Retransmit Timeout");
                size = -1;
                goto out;
            }
            // copying is already done in recv_thread and handler.
            sem_destroy(&tl_msg_list[source].sem);
            free_tl_msg(source);
            wait_idx++;
        }
    }

out:
    /* Clean up any remaining in-flight TL messages on error */
    while (wait_idx < send_idx) {
        int source = source_list[wait_idx];
        sem_destroy(&tl_msg_list[source].sem);
        free_tl_msg(source);
        wait_idx++;
    }

    /* Free the last allocated send_ox that wasn't sent */
    if (send_ox)
        free(send_ox);

    pthread_mutex_unlock(&read_lock);
    return size;
}

/**
 * Wait for a write response with timeout and retransmit loop
 * Similar to wait_for_response but includes data flits in retransmit
 */
static int
wait_for_write_response(int source)
{
    struct timespec timeout;
    int ret;
    int retry_count = 0;
    struct ox_packet_struct * send_ox = NULL;
    uint64_t be64_temp;
    struct tl_flit *tl = &tl_msg_list[source].sent_tl;

    set_timeout(&timeout, NS_PER_SEC);  /* 1 second for writes */
    ret = sem_timedwait(&tl_msg_list[source].sem, &timeout);

//    if (ret == 0)
//        return 0;

    /* Timeout - enter retransmit loop */
    while (ret != 0) {
        retry_count++;
        LOG_DEBUG("WRITE TIMEOUT source=%d offset=%lx - retransmitting (retry %d)",
                  source, tl->tl_addr, retry_count);

        send_ox = alloc_ox_packet_struct();
        tl->flits = send_ox->flits;

        memcpy(&be64_temp, &tl->hdr, sizeof(uint64_t));
            
        tl->flits[0] = be64toh(be64_temp);
        tl->flits[1] = be64toh(tl->tl_addr);

        /* Copy data to flits */
        memcpy(&tl->flits[2], tl->host_buf, tl->size);

        send_ox->tl_msg_mask = 0x1;
        send_ox->flit_cnt = 2 + tl->size / sizeof(uint64_t);

        sem_destroy(&tl_msg_list[source].sem);
        sem_init(&tl_msg_list[source].sem, 0, 0);
        tl_msg_list[source].tl_status = TL_SENT;

        send_ox_packet(send_ox);

        retransmit_count++;
        LOG_DEBUG("WRITE Retransmitted source=%d addr=%lx (retry %d) retransmit total =%ld", source, tl->tl_addr, retry_count, retransmit_count);

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
    struct ox_packet_struct * send_ox;
    size_t bytes_written = 0;
    off_t absolute_offset = bdev->device_base + offset;
    int source_list[MAX_TL_MSG_LIST];
//    size_t chunk_sizes[1024];
    int send_idx = 0;
    int wait_idx = 0;

    /* Bounds check */
    if (offset + size > bdev->device_size) {
        LOG_ERROR("Write out of bounds (device %d)", bdev->device_index);
        return -EINVAL;
    }

    LOG_DEBUG("WRITE offset=%lx size=%ld", offset, size);

    pthread_mutex_lock(&read_lock);

    send_ox = alloc_ox_packet_struct();

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
                free(send_ox);
                return -ENOMEM;
            }
            source_list[send_idx] = source;

            /* Setup TileLink PUTFULLDATA message */
            struct tl_flit *tl = &tl_msg_list[source].sent_tl;
            tl->hdr.source = source;
            tl->hdr.size = log2_size;
            tl->hdr.opcode = A_PUTFULLDATA_OPCODE;
            tl->hdr.chan = CHANNEL_A;
            tl->tl_addr = absolute_offset + bytes_written;
            tl->size = chunk_size;
            tl->flits = send_ox->flits;
            tl->host_buf = (char*)buf + bytes_written;

            uint64_t be64_temp;
            memcpy(&be64_temp, &tl->hdr, sizeof(uint64_t));
            
            tl->flits[0] = be64toh(be64_temp);
            tl->flits[1] = be64toh(tl->tl_addr);

            /* Copy data to flits */
            memcpy(&tl->flits[2], tl->host_buf, tl->size);

            send_ox->tl_msg_mask = 0x1;
            send_ox->flit_cnt = 2 + chunk_size / sizeof(uint64_t);
            tl_msg_list[source].tl_status = TL_SENT;

            sem_init(&tl_msg_list[source].sem, 0, 0);

            //hand over the send_ox to send_thread
            send_ox_packet(send_ox);

//            chunk_sizes[send_idx] = chunk_size;
            bytes_written += chunk_size;
            send_idx++;

            if ( bytes_written < size ) {
                //we need new send_ox
                send_ox = alloc_ox_packet_struct();
            } else
                send_ox = NULL;
        }

        /* Wait for oldest in-flight request */
        if (wait_idx < send_idx) {
            int source = source_list[wait_idx];

            wait_for_write_response(source);

            sem_destroy(&tl_msg_list[source].sem);
            free_tl_msg(source);
            wait_idx++;
        }
    }

    /* Free the last allocated send_ox that wasn't sent */
    if (send_ox)
        free(send_ox);

    pthread_mutex_unlock(&read_lock);

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
                /* Update stats */
                pthread_mutex_lock(&global_stats.lock);
                global_stats.read_count++;
                global_stats.read_bytes += len;
                pthread_mutex_unlock(&global_stats.lock);

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
            if (result > 0) {
                /* Update stats */
                pthread_mutex_lock(&global_stats.lock);
                global_stats.write_count++;
                global_stats.write_bytes += len;
                pthread_mutex_unlock(&global_stats.lock);
            }
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

        /* Convert to wire format. the packet is now placed in send_ox->packet_buffer and size is send_ox->packet_size */
        ox_struct_to_packet(send_ox);

        if (send(global_state.oxmem_info.sockfd, send_ox->packet_buffer, send_ox->packet_size, 0) < 0)
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
    struct ox_packet_struct * recv_ox;

    (void)arg;

    printf("Post-handler thread started\n");

    while (1) {
        recv_ox = (struct ox_packet_struct *)dequeue(&q_recv_post);
        if (recv_ox == NULL) {
            if (shutdown_requested)
                break;
            sem_wait(&recv_post_sem);
            continue;
        }

        /* Parse received packet */
        packet_to_ox_struct(recv_ox);

        /* Update expected sequence number */
        update_seq_num_expected(global_state.oxmem_info.connection_id, recv_ox);

        /* Handle ACK-only packets */
        if (recv_ox->tloe_hdr.msg_type == ACK_ONLY) {
            free(recv_ox);
            continue;
        }

        /* Handle CLOSE_CONN */
        if (recv_ox->tloe_hdr.msg_type == CLOSE_CONN) {
            LOG_DEBUG("CLOSE_CONN packet received");
            free(recv_ox);
            break;
        }

        /* Send ACK if send queue is empty */
        if (isEmpty(&q_send))
            send_ack();

        /* Update remote credit */
        if (recv_ox->tloe_hdr.chan > 0) {
            pthread_mutex_lock(&credit_lock);
            global_state.your_credit_remained[recv_ox->tloe_hdr.chan - 1] +=
                1 << recv_ox->tloe_hdr.credit;
            pthread_mutex_unlock(&credit_lock);
        }

        /* Process TileLink messages */
        if (recv_ox->tl_msg_mask != 0) {
            uint64_t mask = recv_ox->tl_msg_mask;

            pthread_mutex_lock(&credit_lock);
            global_state.my_credit_in_use[CHANNEL_D - 1] +=
                get_used_credit(recv_ox, CHANNEL_D);
            pthread_mutex_unlock(&credit_lock);

            /* Match responses to pending requests */
            for (int i = 0; i < 64 && mask; i++, mask >>= 1) {
                if (!(mask & 0x1))
                    continue;

                struct tl_msg_header_chan_AD tl_msg_header;
                uint64_t be64_temp = be64toh(recv_ox->flits[i]);
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
                    
                    struct tl_flit *tl = &tl_msg_list[source].sent_tl;
                    memcpy(tl->host_buf, &(recv_ox->flits[i+1]), tl->size);
                }

                /* Signal waiting thread */
                tl_msg_list[source].tl_status = TL_RECEIVED;
                sem_post(&tl_msg_list[source].sem);
            }        /* Send ACK if send queue is empty */
        }

        free(recv_ox);
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
    struct ox_packet_struct * recv_ox = NULL;
    char * recv_buffer;
    int recv_size;

    (void)arg;

    printf("Receive thread started\n");

    while (1) {
        if ( recv_ox == NULL)
            recv_ox = alloc_ox_packet_struct();

        recv_buffer = recv_ox->packet_buffer;

        recv_size = recv(global_state.oxmem_info.sockfd, recv_buffer,
                         BUFFER_SIZE, 0);

        if (recv_size < 0) {
            if (shutdown_requested) {
                free(recv_ox);
                break;
            }
            LOG_ERROR("recv failed: %s", strerror(errno));
            continue;
        }

        /* Fast filter: check ethertype */
        struct ethhdr *eth_hdr = (struct ethhdr *)recv_buffer;
        if (eth_hdr->h_proto != OX_ETHERTYPE)
            continue;

        /* Allocate and enqueue work item */
        recv_ox->packet_size = recv_size;

        enqueue(&q_recv_post, (uint64_t)recv_ox);
        recv_ox = NULL;
        sem_post(&recv_post_sem);
    }

    printf("Receive thread exiting\n");
    return NULL;
}

/**
 * Stats thread - periodically writes I/O statistics to JSON file
 */
void *
stats_thread(void *arg)
{
    FILE *fp;
    uint64_t read_count, read_bytes, write_count, write_bytes;
    uint64_t prev_read_count = 0, prev_read_bytes = 0;
    uint64_t prev_write_count = 0, prev_write_bytes = 0;
    uint64_t read_count_per_period, read_bytes_per_period;
    uint64_t write_count_per_period, write_bytes_per_period;
    time_t now;
    char timestamp[64];

    (void)arg;

    printf("Stats thread started (updating %s every %d seconds)\n",
           STATS_FILE_PATH, stats_update_interval);

    while (!shutdown_requested) {
        sleep(stats_update_interval);

        if (shutdown_requested)
            break;

        /* Get current stats with lock */
        pthread_mutex_lock(&global_stats.lock);
        read_count = global_stats.read_count;
        read_bytes = global_stats.read_bytes;
        write_count = global_stats.write_count;
        write_bytes = global_stats.write_bytes;
        pthread_mutex_unlock(&global_stats.lock);

        /* Calculate per-period stats */
        read_count_per_period = read_count - prev_read_count;
        read_bytes_per_period = read_bytes - prev_read_bytes;
        write_count_per_period = write_count - prev_write_count;
        write_bytes_per_period = write_bytes - prev_write_bytes;

        /* Save current values for next period */
        prev_read_count = read_count;
        prev_read_bytes = read_bytes;
        prev_write_count = write_count;
        prev_write_bytes = write_bytes;

        /* Get current timestamp */
        now = time(NULL);
        strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%S", localtime(&now));

        /* Write stats to JSON file */
        fp = fopen(STATS_FILE_PATH, "w");
        if (fp) {
            fprintf(fp, "{\n");
            fprintf(fp, "  \"timestamp\": \"%s\",\n", timestamp);
            fprintf(fp, "  \"interval_seconds\": %d,\n", stats_update_interval);
            fprintf(fp, "  \"read\": {\n");
            fprintf(fp, "    \"count\": %lu,\n", read_count);
            fprintf(fp, "    \"bytes\": %lu,\n", read_bytes);
            fprintf(fp, "    \"count_per_period\": %lu,\n", read_count_per_period);
            fprintf(fp, "    \"bytes_per_period\": %lu\n", read_bytes_per_period);
            fprintf(fp, "  },\n");
            fprintf(fp, "  \"write\": {\n");
            fprintf(fp, "    \"count\": %lu,\n", write_count);
            fprintf(fp, "    \"bytes\": %lu,\n", write_bytes);
            fprintf(fp, "    \"count_per_period\": %lu,\n", write_count_per_period);
            fprintf(fp, "    \"bytes_per_period\": %lu\n", write_bytes_per_period);
            fprintf(fp, "  }\n");
            fprintf(fp, "}\n");
            fclose(fp);
        } else {
            LOG_ERROR("Failed to open %s: %s", STATS_FILE_PATH, strerror(errno));
        }
    }

    printf("Stats thread exiting\n");
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
           "  --netdev=DEV       Network interface\n"
           "  --mac=MAC          MAC address of OmniXtend endpoint\n"
           "  --size=SIZE        Total size with unit G/M/K\n\n"
           "Optional options:\n"
           "  --base=ADDR        Base address in hex (default: 0x0)\n"
           "  --num=N            Number of NBD devices (default: 1, max: %d)\n"
           "  --stat-interval=N  Stats update interval in seconds (default: %d)\n"
           "  -h, --help         Show this help\n\n"
           "Examples:\n"
           "  %s --netdev=eth0 --mac=04:00:00:00:00:00 --size=1G\n"
           "  %s --netdev=eth0 --mac=04:00:00:00:00:00 --size=8G --stat-interval=5\n",
           MAX_NBD_DEVICES, STATS_UPDATE_INTERVAL_DEFAULT, progname, progname);
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
    pthread_t send_tid, recv_tid, post_handler_tid, stats_tid;
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
        else if (strncmp(argv[i], "--stat-interval=", 16) == 0)
            stats_update_interval = atoi(argv[i] + 16);
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

    if (stats_update_interval < 1) {
        LOG_ERROR("--stat-interval must be at least 1 second");
        return 1;
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
        pthread_create(&recv_tid, NULL, recv_thread, NULL) != 0 ||
        pthread_create(&stats_tid, NULL, stats_thread, NULL) != 0) {
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
