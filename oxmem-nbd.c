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

// Logging infrastructure
#define LOG_INFO(fmt, ...) printf("[INFO] " fmt "\n", ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) fprintf(stderr, "[ERROR] " fmt "\n", ##__VA_ARGS__)
#if 0
#define LOG_DEBUG(fmt, ...) printf("[DEBUG] %s %d " fmt "\n", __func__, __LINE__, ##__VA_ARGS__)
#else
#define LOG_DEBUG(fmt, ...) 
#endif
#define LOG_THREAD(name, fmt, ...) printf("[%s] " fmt "\n", name, ##__VA_ARGS__)

// NBD device path
#define NBD_DEVICE "/dev/nbd0"
#define READ_WRITE_UNIT 1024
#define RESPONSE_TIMEOUT_SEC 10
#define MAX_NBD_DEVICES 16
#define MAX_TL_MSG_LIST 1000

//MAX_INFLIGHT_READS>1 makes some receiving packets are dropped or its head part is missing.
#define MAX_INFLIGHT_READS 1
#define MAX_INFLIGHT_WRITES 1024

// Per-NBD device state
struct oxmem_bdev
{
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
struct oxmem_global
{
  // Network (shared across all NBD devices)
  struct oxmem_info_struct oxmem_info;
  unsigned int my_credit_in_use[5];
  unsigned int your_credit_remained[5];

  // Multiple NBD devices
  struct oxmem_bdev bdevs[MAX_NBD_DEVICES];
  int num_devices;
};

struct tl_flit
{
  struct tl_msg_header_chan_AD hdr;
  uint64_t flits[READ_WRITE_UNIT / sizeof (uint64_t) + 2];
};

enum TL_status
{
  TL_FREE,
  TL_IN_USE,
  TL_SENT,
  TL_RECEIVED
};

struct tl_msg_list_entry
{
  struct tl_flit sent_tl;
  struct tl_flit recv_tl;
  int tl_status;
  sem_t sem;
};

// Work item for post-processing received packets
struct recv_work_item
{
  char recv_buffer[BUFFER_SIZE];
  int recv_size;
  uint64_t id;
};

// Global variables
struct tl_msg_list_entry tl_msg_list[MAX_TL_MSG_LIST];
static pthread_mutex_t tl_msg_list_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t credit_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t send_lock = PTHREAD_MUTEX_INITIALIZER;
static volatile sig_atomic_t shutdown_requested = 0;
static struct oxmem_global global_state;
static Queue q_send;
static sem_t send_sem;
static Queue q_recv_post;
static sem_t recv_post_sem;

// Function declarations
void *nbd_server_thread (void *arg);
void *recv_thread (void *arg);
void *post_handler_thread (void *arg);
static void cleanup_and_exit (void);
static int oxmem_read_blocking (char *buf, size_t size, off_t offset,
				struct oxmem_bdev *bdev);
static int oxmem_write_blocking (const char *buf, size_t size, off_t offset,
				 struct oxmem_bdev *bdev);

void
init_tl_msg_list (void)
{
  memset (tl_msg_list, 0, sizeof (tl_msg_list));
}

/* get a free tl_msg entry */
int
get_tl_msg (void)
{
  int i;

  pthread_mutex_lock (&tl_msg_list_lock);

  for (i = 0; i < MAX_TL_MSG_LIST; i++)
    {
      if (tl_msg_list[i].tl_status == TL_FREE)
	{
	  tl_msg_list[i].tl_status = TL_IN_USE;
	  break;
	}
    }

  pthread_mutex_unlock (&tl_msg_list_lock);

  if (i >= MAX_TL_MSG_LIST)
    i = -1;

  return i;
}

/* free a tl_msg entry */
void
free_tl_msg (int i)
{
  pthread_mutex_lock (&tl_msg_list_lock);
  if (i >= 0 && i < MAX_TL_MSG_LIST && tl_msg_list[i].tl_status != TL_FREE)
    memset (&tl_msg_list[i], 0, sizeof (struct tl_msg_list_entry));
  pthread_mutex_unlock (&tl_msg_list_lock);
}

/**
 * Signal handler for graceful shutdown
 */
static void
signal_handler (int sig)
{
  int i;
  (void) sig;
  shutdown_requested = 1;

  // Cleanup
  cleanup_and_exit ();

    for (i = 0; i < global_state.num_devices; i++) {
	    if (global_state.bdevs[i].nbd_fd > 0) {
	        ioctl(global_state.bdevs[i].nbd_fd, NBD_DISCONNECT);
	    }
    }
  exit (0);
}

int
get_used_credit (struct ox_packet_struct *recv_ox, int channel)
{
  int credit = 0;
  int i;
  struct tl_msg_header_chan_AD tl_msg_ack;
  uint64_t be64_temp, tl_msg_mask;

  tl_msg_mask = recv_ox->tl_msg_mask;

  for (i = 0; i < 64; i++)
    {
      if (tl_msg_mask & 0x1)
	{
	  credit++;
	  be64_temp = be64toh (recv_ox->flits[i]);
	  memcpy (&tl_msg_ack, &be64_temp, sizeof (uint64_t));
	  if (tl_msg_ack.chan == channel &&
	      tl_msg_ack.opcode == D_ACCESSACKDATA_OPCODE)
	    {
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


int
get_ack_credit (int channel)
{
  int i;
  int credit;

  if (channel < CHANNEL_A || channel > CHANNEL_E)
    return 0;

  pthread_mutex_lock (&credit_lock);

  // Round down to power of 2
  for (i = 31; i >= 0; i--)
    {
      if (global_state.my_credit_in_use[channel - 1] >= (1UL << i))
	{
	  break;
	}
    }

  credit = i;

  if (i >= 0)
    //reduce credit_in_use
    global_state.my_credit_in_use[channel - 1] -= (1UL << i);

  pthread_mutex_unlock (&credit_lock);

  return credit;

}

void
update_send_ox_credit (struct ox_packet_struct *send_ox)
{
  int credit;

  if (send_ox->tloe_hdr.chan == 0)
    {
      credit = get_ack_credit (CHANNEL_D);
      if (credit >= 0)
	{
	  send_ox->tloe_hdr.credit = credit;
	  send_ox->tloe_hdr.chan = CHANNEL_D;
	}
    }
}

/* send_ox - send ox_packet */
int
send_ox_packet (struct ox_packet_struct *send_ox)
{
  char send_buffer[BUFFER_SIZE];
  int send_size;
  int ret;
  struct ox_packet_struct *send_ox_buffer;

  send_ox_buffer = malloc (sizeof (struct ox_packet_struct));
  pthread_mutex_lock (&send_lock);
  memcpy (send_ox_buffer, send_ox, sizeof (struct ox_packet_struct));
  pthread_mutex_unlock (&send_lock);

  if (send_ox_buffer->tloe_hdr.msg_type != OPEN_CONN)
    {
      send_ox_buffer->tloe_hdr.ack = 1;
    }

  enqueue (&q_send, (uint64_t) send_ox_buffer);

  sem_post (&send_sem);

  return 0;
}

void
send_disconnection (void)
{
  struct ox_packet_struct send_ox;

  //setup ETH header
  setup_send_ox_eth_hdr (global_state.oxmem_info.connection_id, &send_ox);

  send_ox.tloe_hdr.msg_type = CLOSE_CONN;

  send_ox_packet (&send_ox);

  usleep(100000);
}

int
send_open_connection (uint64_t my_mac, uint64_t dst_mac)
{
  struct ox_packet_struct send_ox;
  int connection_id;

  //Add new connection
  if (connection_id = add_new_connection (dst_mac, my_mac, 0x3FFFFF) < 0)
    {
      LOG_ERROR ("connection creation failed.");
      return -1;
    }

  global_state.oxmem_info.connection_id = connection_id;

  //setup ETH header
  setup_send_ox_eth_hdr (connection_id, &send_ox);

  //setup OX header
  send_ox.tloe_hdr.chan = CHANNEL_A;
  send_ox.tloe_hdr.credit = DEFAULT_CREDIT;
  send_ox.tloe_hdr.msg_type = OPEN_CONN;

  // Send on Channel A
  send_ox_packet (&send_ox);

  // Send on Channel D
  send_ox.tloe_hdr.msg_type = NORMAL;
  send_ox.tloe_hdr.chan = CHANNEL_D;
  send_ox_packet (&send_ox);

  return 0;
}

int
send_ack (void)
{
  struct ox_packet_struct send_ox;

  //setup ETH header
  setup_send_ox_eth_hdr (global_state.oxmem_info.connection_id, &send_ox);

  send_ox_packet (&send_ox);

}


/**
 * Cleanup and exit
 */
static void
cleanup_and_exit (void)
{
  struct ox_packet_struct send_ox, recv_ox;
  int i;

  // Send disconnect packet
  if (global_state.oxmem_info.connection_id >= 0)
    {
      printf ("\nClosing OmniXtend connection...\n");

      // Try to send disconnect (ignore errors during shutdown)
      send_disconnection ();

      delete_connection (global_state.oxmem_info.connection_id);
      global_state.oxmem_info.connection_id = -1;
    }
  // Cleanup all NBD devices
  for (i = 0; i < global_state.num_devices; i++)
    {
      struct oxmem_bdev *bdev = &global_state.bdevs[i];

      printf ("\nClosing NBD sockets...\n");

      if (bdev->nbd_fd > 0)
	{
	  ioctl (bdev->nbd_fd, NBD_CLEAR_QUE);
          ioctl(bdev->nbd_fd, NBD_CLEAR_SOCK);
	  close (bdev->nbd_fd);
	}
      // Close sockets
      if (bdev->sk_pair[0] > 0)
	{
	  close (bdev->sk_pair[0]);
	  close (bdev->sk_pair[1]);
	}
    }

  if (global_state.oxmem_info.sockfd > 0)
    {
      close (global_state.oxmem_info.sockfd);
    }

  printf ("Cleanup complete\n");
}

/**
 * Read from OmniXtend memory with flow control
 * Uses sliding window with MAX_INFLIGHT_READS for better flow control
 */

static int
oxmem_read_blocking (char *buf, size_t size, off_t offset,
		     struct oxmem_bdev *bdev)
{
  struct ox_packet_struct send_ox;
  size_t bytes_processed = 0;
  size_t chunk_size;
  int ret, i;
  off_t absolute_offset = bdev->device_base + offset;
  int source;
  uint64_t be64_temp;
  struct timespec timeout;
  int source_list[1024];
  size_t chunk_sizes[1024];
  size_t chunk_offsets[1024];
  int send_idx = 0;		// Next position to send
  int wait_idx = 0;		// Next position to wait for

  LOG_DEBUG ("READ offset=%lx size=%ld", offset, size);

  // Bounds check
  if (offset + size > bdev->device_size)
    {
      fprintf (stderr, "Read out of bounds (device %d)\n",
	       bdev->device_index);
      return -EINVAL;
    }
  // Connection check
  if (global_state.oxmem_info.connection_id < 0)
    {
      fprintf (stderr, "Not connected\n");
      return -ENXIO;
    }

  //setup ETH header
  setup_send_ox_eth_hdr (global_state.oxmem_info.connection_id, &send_ox);

  // Read in chunks with sliding window flow control
  while (bytes_processed < size || wait_idx < send_idx)
    {
      // Send new requests if we haven't finished and have room in the window
      while (bytes_processed < size
	     && (send_idx - wait_idx) < MAX_INFLIGHT_READS)
	{
	  chunk_size = size - bytes_processed;
	  if (chunk_size > READ_WRITE_UNIT)
	    chunk_size = READ_WRITE_UNIT;

	  // Round down to power of 2
	  for (i = 10; i >= 0; i--)
	    {
	      if (chunk_size >= (1 << i))
		{
		  chunk_size = 1 << i;
		  break;
		}
	    }

	  source = source_list[send_idx] = get_tl_msg ();

	  //setup TileLink message (GET)
	  tl_msg_list[source].sent_tl.hdr.source = source;
	  tl_msg_list[source].sent_tl.hdr.size = i;
	  tl_msg_list[source].sent_tl.hdr.opcode = A_GET_OPCODE;
	  tl_msg_list[source].sent_tl.hdr.chan = CHANNEL_A;

	  //setup flits of send_ox
	  memcpy (&be64_temp, &tl_msg_list[source].sent_tl.hdr,
		  sizeof (uint64_t));
	  tl_msg_list[source].sent_tl.flits[0] = be64toh (be64_temp);

	  //setup address
	  be64_temp = absolute_offset + bytes_processed;
	  tl_msg_list[source].sent_tl.flits[1] = be64toh (be64_temp);

	  send_ox.tl_msg_mask = 0x1;
	  send_ox.flit_cnt = 2;
	  send_ox.flits = tl_msg_list[source].sent_tl.flits;
	  tl_msg_list[source].tl_status = TL_SENT;

	  sem_init (&tl_msg_list[source].sem, 0, 0);
	  send_ox_packet (&send_ox);

	  // Track chunk information for later data copy
	  chunk_sizes[send_idx] = chunk_size;
	  chunk_offsets[send_idx] = bytes_processed;

	  bytes_processed += chunk_size;
	  send_idx++;
	}

      // Wait for oldest in-flight request to complete
      if (wait_idx < send_idx)
	{
	  source = source_list[wait_idx];

	  clock_gettime (CLOCK_REALTIME, &timeout);
	  timeout.tv_sec += 1;

	  ret = sem_timedwait (&tl_msg_list[source].sem, &timeout);
	  if (ret)
	    {
	      LOG_DEBUG ("READ sem_timedwait TIMEOUT source=%d offset=%lx - retransmitting",
	                 source, absolute_offset+chunk_offsets[wait_idx]);

//signal_handler(1);

	      // Retransmit: Reconstruct send_ox from tl_msg_list[source]
	      send_ox.tl_msg_mask = 0x1;
	      send_ox.flit_cnt = 2;
	      send_ox.flits = tl_msg_list[source].sent_tl.flits;

	      // Reset semaphore and status for retry
	      sem_destroy (&tl_msg_list[source].sem);
	      sem_init (&tl_msg_list[source].sem, 0, 0);
	      tl_msg_list[source].tl_status = TL_SENT;

	      // Retransmit the packet
	      send_ox_packet (&send_ox);

	      LOG_DEBUG ("Retransmitted source=%d, waiting again", source);

	      // Wait again for the retransmitted request
	      clock_gettime (CLOCK_REALTIME, &timeout);
	      timeout.tv_sec += 1;
	      ret = sem_timedwait (&tl_msg_list[source].sem, &timeout);

	      if (ret)
		{
		  LOG_ERROR ("TIMEOUT again after retransmit source=%d offset=%lx",
		             source, absolute_offset+chunk_offsets[wait_idx]);
		}
	    }

	  // Copy data to correct buffer position
	  memcpy (buf + chunk_offsets[wait_idx],
		  &tl_msg_list[source].recv_tl.flits[1],
		  chunk_sizes[wait_idx]);

	  sem_destroy (&tl_msg_list[source].sem);
	  free_tl_msg (source);
	  wait_idx++;
	}
    }

  return size;
}

/**
 * Write to OmniXtend memory with flow control
 * Limits in-flight requests to MAX_INFLIGHT_WRITES for better flow control
 */

static int
oxmem_write_blocking (const char *buf, size_t size, off_t offset,
		      struct oxmem_bdev *bdev)
{
  struct ox_packet_struct send_ox;
  uint64_t send_flits[256];
  size_t bytes_written = 0;
  size_t chunk_size;
  int ret, i;
  off_t absolute_offset = bdev->device_base + offset;
  int source;
  uint64_t be64_temp;
  struct timespec timeout;
  int source_list[1024];
  size_t chunk_sizes[1024];	// Track chunk sizes for retry
  int send_idx = 0;		// Next position to send
  int wait_idx = 0;		// Next position to wait for

  LOG_DEBUG ("WRITE offset=%lx size=%ld", offset, size);

  // Bounds check
  if (offset + size > bdev->device_size)
    {
      fprintf (stderr, "Write out of bounds (device %d)\n",
	       bdev->device_index);
      return -EINVAL;
    }

  //setup ETH header
  setup_send_ox_eth_hdr (global_state.oxmem_info.connection_id, &send_ox);

  // Write in chunks with sliding window flow control
  while (bytes_written < size || wait_idx < send_idx)
    {
      // Send new requests if we haven't finished and have room in the window
      while (bytes_written < size
	     && (send_idx - wait_idx) < MAX_INFLIGHT_WRITES)
	{
	  chunk_size = size - bytes_written;
	  if (chunk_size > READ_WRITE_UNIT)
	    chunk_size = READ_WRITE_UNIT;

	  // Round down to power of 2
	  for (i = 10; i >= 0; i--)
	    {
	      if (chunk_size >= (1 << i))
		{
		  chunk_size = 1 << i;
		  break;
		}
	    }

	  source = source_list[send_idx] = get_tl_msg ();

	  //setup TileLink message (PUTFULL)
	  tl_msg_list[source].sent_tl.hdr.source = source;
	  tl_msg_list[source].sent_tl.hdr.size = i;
	  tl_msg_list[source].sent_tl.hdr.opcode = A_PUTFULLDATA_OPCODE;
	  tl_msg_list[source].sent_tl.hdr.chan = CHANNEL_A;

	  //setup flits of send_ox
	  memcpy (&be64_temp, &tl_msg_list[source].sent_tl.hdr,
		  sizeof (uint64_t));
	  tl_msg_list[source].sent_tl.flits[0] = be64toh (be64_temp);

	  be64_temp = absolute_offset + bytes_written;
	  tl_msg_list[source].sent_tl.flits[1] = be64toh (be64_temp);

	  memcpy (&tl_msg_list[source].sent_tl.flits[2], buf + bytes_written,
		  chunk_size);

	  send_ox.tl_msg_mask = 0x1;
	  send_ox.flit_cnt = 2 + chunk_size / sizeof (uint64_t);
	  send_ox.flits = tl_msg_list[source].sent_tl.flits;
	  tl_msg_list[source].tl_status = TL_SENT;

	  sem_init (&tl_msg_list[source].sem, 0, 0);
	  send_ox_packet (&send_ox);

	  // Track chunk size for potential retry
	  chunk_sizes[send_idx] = chunk_size;

	  bytes_written += chunk_size;
	  send_idx++;
	}

      // Wait for oldest in-flight request to complete
      if (wait_idx < send_idx)
	{
	  source = source_list[wait_idx];

	  clock_gettime (CLOCK_REALTIME, &timeout);
	  timeout.tv_sec += 1;

	  ret = sem_timedwait (&tl_msg_list[source].sem, &timeout);
	  if (ret)
	    {
	  be64_temp = tl_msg_list[source].sent_tl.flits[1] = be64toh (be64_temp);
	  be64_temp = be64toh(be64_temp);
	      LOG_DEBUG ("sem_timedwait TIMEOUT source=%d offset=%lx - retransmitting",
	                 source, be64_temp);

//signal_handler(1);
	      // Retransmit: Reconstruct send_ox from tl_msg_list[source]
	      send_ox.tl_msg_mask = 0x1;
	      send_ox.flit_cnt = 2 + chunk_sizes[wait_idx] / sizeof (uint64_t);
	      send_ox.flits = tl_msg_list[source].sent_tl.flits;

	      // Reset semaphore and status for retry
	      sem_destroy (&tl_msg_list[source].sem);
	      sem_init (&tl_msg_list[source].sem, 0, 0);
	      tl_msg_list[source].tl_status = TL_SENT;

	      // Retransmit the packet
	      send_ox_packet (&send_ox);

	      LOG_DEBUG ("Retransmitted source=%d, waiting again", source);

	      // Wait again for the retransmitted request
	      clock_gettime (CLOCK_REALTIME, &timeout);
	      timeout.tv_sec += 1;
	      ret = sem_timedwait (&tl_msg_list[source].sem, &timeout);

	      if (ret)
		{
		  LOG_ERROR ("TIMEOUT again after retransmit source=%d", source);
		}
	    }

	  sem_destroy (&tl_msg_list[source].sem);
	  free_tl_msg (source);
	  wait_idx++;
	}
    }

  return size;
}

/**
 * NBD_DO_IT thread - one per device
 * This thread blocks in NBD_DO_IT ioctl until the device is disconnected
 */
void *
nbd_do_it_thread (void *arg)
{
  struct oxmem_bdev *bdev = (struct oxmem_bdev *) arg;

  printf ("NBD_DO_IT thread started for %s\n", bdev->device_path);

  // This blocks until NBD_DISCONNECT is called
  if (ioctl (bdev->nbd_fd, NBD_DO_IT) < 0)
    {
      if (!shutdown_requested)
	{
	  fprintf (stderr, "NBD_DO_IT failed for %s: %s\n",
		   bdev->device_path, strerror (errno));
	}
    }

  printf ("NBD_DO_IT thread exiting for %s\n", bdev->device_path);
  return NULL;
}

/**
 * NBD server thread - handles NBD requests
 * One thread per NBD device
 */
void *
nbd_server_thread (void *arg)
{
  struct oxmem_bdev *bdev = (struct oxmem_bdev *) arg;
  struct nbd_request request;
  struct nbd_reply reply;
  char *buffer;
  ssize_t bytes_read;

  buffer = malloc (1024 * 1024);	// 1MB buffer
  if (!buffer)
    {
      fprintf (stderr, "Failed to allocate buffer for device %d\n",
	       bdev->device_index);
      return NULL;
    }

  printf ("NBD server thread started for %s\n", bdev->device_path);

  while (!shutdown_requested)
    {
      // Read NBD request
      bytes_read = read (bdev->sk_pair[1], &request, sizeof (request));

      if (bytes_read == 0)
	{
	  printf ("NBD connection closed for %s\n", bdev->device_path);
	  break;
	}

      if (bytes_read < 0)
	{
	  if (errno == EINTR)
	    continue;
	  perror ("Failed to read NBD request");
	  break;
	}

      if (bytes_read != sizeof (request))
	{
	  fprintf (stderr, "Incomplete NBD request\n");
	  continue;
	}
      // Parse request
      uint32_t type = ntohl (request.type);
      uint64_t offset = be64toh (*(uint64_t *) & request.from);
      uint32_t len = ntohl (request.len);

      // Prepare reply
      memset (&reply, 0, sizeof (reply));
      reply.magic = htonl (NBD_REPLY_MAGIC);
      memcpy (reply.handle, request.handle, sizeof (reply.handle));
      reply.error = 0;

      // Handle request
      switch (type)
	{
	case NBD_CMD_READ:
	  {
	    int result = oxmem_read_blocking (buffer, len, offset, bdev);

	    if (result > 0)
	      {
		reply.error = 0;
		write (bdev->sk_pair[1], &reply, sizeof (reply));
		write (bdev->sk_pair[1], buffer, len);
	      }
	    else
	      {
		reply.error = htonl (EIO);
		write (bdev->sk_pair[1], &reply, sizeof (reply));
	      }
	    break;
	  }

	case NBD_CMD_WRITE:
	  {
	    // Read write data
	    ssize_t total_bytes = 0;
	    do
	      {
		bytes_read =
		  read (bdev->sk_pair[1], buffer + total_bytes,
			len - total_bytes);
		if (bytes_read < 0)
		  {
		    reply.error = htonl (EIO);
		    write (bdev->sk_pair[1], &reply, sizeof (reply));
		    break;
		  }
		total_bytes += bytes_read;
	      }
	    while (total_bytes < len);

	    if (total_bytes < len)
	      break;

	    int result = oxmem_write_blocking (buffer, len, offset, bdev);

	    if (result > 0)
	      {
		reply.error = 0;
	      }
	    else
	      {
		reply.error = htonl (EIO);
	      }

	    write (bdev->sk_pair[1], &reply, sizeof (reply));
	    break;
	  }

	case NBD_CMD_DISC:
	  printf ("NBD disconnect requested for %s\n", bdev->device_path);
	  goto cleanup;

	case NBD_CMD_FLUSH:
	  // No-op for network device
	  write (bdev->sk_pair[1], &reply, sizeof (reply));
	  break;

	default:
	  printf ("Unknown NBD command: %u\n", type);
	  reply.error = htonl (EINVAL);
	  write (bdev->sk_pair[1], &reply, sizeof (reply));
	  break;
	}
    }

cleanup:
  free (buffer);
  printf ("NBD server thread exiting for %s\n", bdev->device_path);
  return NULL;
}

void *
send_thread (void *arg)
{
  char send_buffer[BUFFER_SIZE];
  int send_size;
  int ret;
  struct ox_packet_struct *send_ox = NULL;
  int stop = 0;
  sem_init (&send_sem, 0, 0);

//  while (!shutdown_requested)
  while (!stop)
    {

      //dequeue a ox_packet_struct
      send_ox = (struct ox_packet_struct *) dequeue (&q_send);

      if (send_ox == NULL)
	{
	  sem_wait (&send_sem);
	  continue;
	}
      // Set sequence number, credit and convert to packet
      set_seq_num_to_ox_packet (global_state.oxmem_info.connection_id,
				send_ox);

      update_send_ox_credit (send_ox);

      ox_struct_to_packet (send_ox, send_buffer, &send_size);

      // Send packet
      ret = send (global_state.oxmem_info.sockfd, send_buffer, send_size, 0);

      if (ret < 0)
	{
	  LOG_ERROR ("send failed");
	}

      if (send_ox->tloe_hdr.msg_type == CLOSE_CONN) {
        LOG_DEBUG("CLOSE CONN packet sent.");
        stop = 1;
      }

      free (send_ox);
    }

  sem_destroy (&send_sem);

  return NULL;
}

/**
 * Post-handler thread - processes received packets
 * This thread does all the heavy work that was previously in recv_thread
 */
void *
post_handler_thread (void *arg)
{
  struct recv_work_item *work_item;
  struct ox_packet_struct recv_ox;
  struct tl_msg_header_chan_AD tl_msg_header;
  uint64_t be64_temp;
  int source;

  (void) arg;			// Unused parameter

  printf ("Post-handler thread started\n");

  while (1)
    {

      // Dequeue work item
      work_item = (struct recv_work_item *) dequeue (&q_recv_post);
      if (work_item == NULL)
	{
	  if (shutdown_requested)
	    break;
      // Wait for work from recv_thread
      sem_wait (&recv_post_sem);
	  continue;
	}

      // Parse response packet (previously line 857)
      packet_to_ox_struct (work_item->recv_buffer, work_item->recv_size,
			    &recv_ox);

    if ( work_item->recv_size > 70)
    LOG_DEBUG ("work_item->recv_size = %d id = %lu", work_item->recv_size, work_item->id);

      // Update expected sequence number (previously lines 867-873)
      if (0 >
	  update_seq_num_expected (global_state.oxmem_info.connection_id,
				   &recv_ox))
	{
	  // Obsolete packet - could continue or process anyway
	}


      // Check message type (previously lines 859-865)
      if (recv_ox.tloe_hdr.msg_type == ACK_ONLY)
	{
	  free (work_item);
	  continue;
	}

      if (recv_ox.tloe_hdr.msg_type == CLOSE_CONN)
	{
	  LOG_DEBUG ("CLOSE CONN packet received.");
	  free (work_item);
	  break;
	}


      if (  isEmpty(&q_send) && global_state.my_credit_in_use[CHANNEL_D - 1] > 16 )
      // Send ACK (previously line 877)
        send_ack ();

      // Update your_credit_remained (previously lines 880-885)
      if (recv_ox.tloe_hdr.chan > 0)
	{
	  pthread_mutex_lock (&credit_lock);
	  global_state.your_credit_remained[recv_ox.tloe_hdr.chan - 1] +=
	    1 << recv_ox.tloe_hdr.credit;
	  pthread_mutex_unlock (&credit_lock);
	}

      // Process TileLink messages (previously lines 887-950)
      if (recv_ox.tl_msg_mask != 0)
	{
	  int i;
	  uint64_t mask = recv_ox.tl_msg_mask;

	  // Update my_credit_in_use (previously lines 895-902)
	  pthread_mutex_lock (&credit_lock);
	  global_state.my_credit_in_use[CHANNEL_D - 1] +=
	    get_used_credit (&recv_ox, CHANNEL_D);
	  pthread_mutex_unlock (&credit_lock);

	  // Source matching loop (previously lines 904-948)
	  for (i = 0; i < 64; i++)
	    {
	      if ((mask >> i) & 0x1)
		{
		  // Extract TileLink header
		  be64_temp = be64toh (recv_ox.flits[i]);
		  memcpy (&tl_msg_header, &be64_temp, sizeof (uint64_t));
		  source = tl_msg_header.source;
		  LOG_DEBUG ("2 source=%d received", source);

		  if (source >= MAX_TL_MSG_LIST)
		    {
		      LOG_DEBUG
			("source = %d is too big mask=%lx tl_msg=%lx recv_size=%d i=%d\n",
			 source, mask, recv_ox.flits[i],
			 work_item->recv_size, i);
		      continue;
		    }

		  // If not sent tl msg, ignore it
		  if (tl_msg_list[source].tl_status != TL_SENT)
		    {
		      continue;
		    }

		  // Copy TileLink header
		  memcpy (&tl_msg_list[source].recv_tl.hdr,
			  &tl_msg_header,
			  sizeof (struct tl_msg_header_chan_AD));

		  // Copy data if ACCESSACKDATA
		  if (tl_msg_header.chan == CHANNEL_D
		      && tl_msg_header.opcode == D_ACCESSACKDATA_OPCODE)
		    {
		      memcpy (&tl_msg_list[source].recv_tl.flits[0],
			      &recv_ox.flits[i],
			      (1 << tl_msg_header.size) + sizeof (uint64_t));
		    }

		  // Update status and signal waiting thread
		  tl_msg_list[source].tl_status = TL_RECEIVED;
		  sem_post (&tl_msg_list[source].sem);
		}

	      if ((mask >> i) == 0)
		break;
	    }
	}

      // Free work item
      free (work_item);
    }

  printf ("Post-handler thread exiting\n");
  return NULL;
}

/**
 * Receive thread - FAST PATH ONLY
 * This thread now does minimal work and immediately returns to recv()
 * All heavy processing is done by post_handler_thread
 */

uint64_t recv_id = 0;

void *
recv_thread (void *arg)
{
  char recv_buffer[BUFFER_SIZE];
  int recv_size;
  struct recv_work_item *work_item;

  (void) arg;			// Unused parameter

  printf ("Receive thread started\n");

  while (1)
    {
      // CRITICAL PATH: Blocking receive
      recv_size = recv (global_state.oxmem_info.sockfd, recv_buffer,
			BUFFER_SIZE, 0);

      if (recv_size < 0)
	{
	  if (shutdown_requested)
	    break;
	  perror ("recv failed");
	  continue;
	}

      // FAST FILTER: Check ethertype
      struct ethhdr *etherHeader = (struct ethhdr *) recv_buffer;
      if (etherHeader->h_proto != OX_ETHERTYPE)
	{
	  // Not an OmniXtend packet, ignore
	  continue;
	}

      // Allocate work item for post-processing
      work_item = (struct recv_work_item *) malloc (sizeof (struct recv_work_item));
      if (work_item == NULL)
	{
	  LOG_ERROR ("Failed to allocate recv work item");
	  continue;
	}

      work_item->id = recv_id++;

    if ( recv_size > 70)
LOG_DEBUG("recv_size = %d id = %lu", recv_size, work_item->id);

      // Copy packet to work item
      memcpy (work_item->recv_buffer, recv_buffer, recv_size);
      work_item->recv_size = recv_size;

      // Enqueue for post-processing
      enqueue (&q_recv_post, (uint64_t) work_item);
      sem_post (&recv_post_sem);

      // IMMEDIATELY RETURN TO recv()
    }

  printf ("Receive thread exiting\n");
  return NULL;
}

/**
 * Parse size with unit (G/M/K)
 */
static size_t
parse_size_with_unit (const char *size_str)
{
  if (!size_str)
    return 0;

  char *end;
  unsigned long long value = strtoull (size_str, &end, 10);
  if (end == size_str)
    return 0;

  size_t multiplier = 1;
  if (*end != '\0')
    {
      switch (*end)
	{
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
static void
show_help (const char *progname)
{
  printf ("Usage: %s [options]\n\n", progname);
  printf ("Simple OmniXtend block device driver\n\n");
  printf ("Required options:\n"
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
int
main (int argc, char *argv[])
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
  uint64_t my_mac;

  // Parse arguments
  for (i = 1; i < argc; i++)
    {
      if (strncmp (argv[i], "--netdev=", 9) == 0)
	{
	  netdev = argv[i] + 9;
	}
      else if (strncmp (argv[i], "--mac=", 6) == 0)
	{
	  mac_str = argv[i] + 6;
	}
      else if (strncmp (argv[i], "--size=", 7) == 0)
	{
	  size_str = argv[i] + 7;
	}
      else if (strncmp (argv[i], "--base=", 7) == 0)
	{
	  base_str = argv[i] + 7;
	}
      else if (strncmp (argv[i], "--num=", 6) == 0)
	{
	  num_str = argv[i] + 6;
	}
      else if (strcmp (argv[i], "-h") == 0 || strcmp (argv[i], "--help") == 0)
	{
	  show_help (argv[0]);
	  return 0;
	}
    }

  // Validate
  if (!netdev || !mac_str || !size_str)
    {
      fprintf (stderr, "Error: Missing required arguments\n");
      show_help (argv[0]);
      return 1;
    }
  // Parse num_devices
  if (num_str)
    {
      num_devices = atoi (num_str);
      if (num_devices < 1 || num_devices > MAX_NBD_DEVICES)
	{
	  fprintf (stderr, "Error: --num must be between 1 and %d\n",
		   MAX_NBD_DEVICES);
	  return 1;
	}
    }
  // Setup signal handlers
  struct sigaction sa;
  memset (&sa, 0, sizeof (sa));
  sa.sa_handler = signal_handler;
  sigemptyset (&sa.sa_mask);
  sigaction (SIGINT, &sa, NULL);
  sigaction (SIGTERM, &sa, NULL);

  // Initialize global state
  memset (&global_state, 0, sizeof (global_state));
  global_state.num_devices = num_devices;

  strncpy (global_state.oxmem_info.netdev, netdev,
	   sizeof (global_state.oxmem_info.netdev) - 1);
  global_state.oxmem_info.netdev_id = if_nametoindex (netdev);

  if (global_state.oxmem_info.netdev_id == 0)
    {
      fprintf (stderr, "Error: Invalid network device: %s\n", netdev);
      return 1;
    }
  // Parse MAC
  if (sscanf (mac_str, "%2x:%2x:%2x:%2x:%2x:%2x",
	      &mac_values[0], &mac_values[1], &mac_values[2],
	      &mac_values[3], &mac_values[4], &mac_values[5]) != 6)
    {
      fprintf (stderr, "Error: Invalid MAC address\n");
      return 1;
    }

  for (i = 0; i < 6; i++)
    {
      global_state.oxmem_info.mac_addr += (uint64_t) mac_values[i] << (i * 8);
    }

  // Parse base and size
  global_state.oxmem_info.base = strtoull (base_str, NULL, 16);
  total_size = parse_size_with_unit (size_str);
  if (total_size == 0)
    {
      fprintf (stderr, "Error: Invalid size\n");
      return 1;
    }
  // Calculate per-device size
  device_size = total_size / num_devices;
  if (device_size == 0)
    {
      fprintf (stderr, "Error: Total size too small for %d devices\n",
	       num_devices);
      return 1;
    }

  global_state.oxmem_info.st.st_size = global_state.oxmem_info.size =
    total_size;
  global_state.oxmem_info.connection_id = -1;

  printf ("OmniXtend Block Device:\n");
  printf ("  Network: %s\n", netdev);
  printf ("  MAC: %s\n", mac_str);
  printf ("  Total Size: %zu bytes (%zu MB)\n", total_size,
	  total_size / (1024 * 1024));
  printf ("  Base: 0x%lx\n", global_state.oxmem_info.base);
  printf ("  Number of devices: %d\n", num_devices);
  printf ("  Size per device: %zu bytes (%zu MB)\n", device_size,
	  device_size / (1024 * 1024));

  // Initialize tl_msg array
  init_tl_msg_list ();

  // Create socket
  global_state.oxmem_info.sockfd =
//    socket (AF_PACKET, SOCK_RAW, htons (ETH_P_ALL));
    socket (AF_PACKET, SOCK_RAW, htons (OX_ETHERTYPE));
  if (global_state.oxmem_info.sockfd < 0)
    {
      perror ("Socket creation failed");
      return 1;
    }
  // Bind socket
  memset (&saddr, 0, sizeof (saddr));
  saddr.sll_family = AF_PACKET;
  saddr.sll_protocol = htons (ETH_P_ALL);
  saddr.sll_ifindex = global_state.oxmem_info.netdev_id;

  if (bind
      (global_state.oxmem_info.sockfd, (struct sockaddr *) &saddr,
       sizeof (saddr)) < 0)
    {
      perror ("Socket bind failed");
      close (global_state.oxmem_info.sockfd);
      return 1;
    }

    //increase socket buffer size
    int sockbuf_size = 4*1024*1024;
    setsockopt(global_state.oxmem_info.sockfd, SOL_SOCKET, SO_RCVBUF, &sockbuf_size, sizeof(sockbuf_size));
    setsockopt(global_state.oxmem_info.sockfd, SOL_SOCKET, SO_SNDBUF, &sockbuf_size, sizeof(sockbuf_size));
    
  // Initialize send queue and start send thread
  initQueue (&q_send);
  pthread_t send_tid;
  if (pthread_create (&send_tid, NULL, send_thread, NULL) != 0)
    {
      perror ("Failed to create send thread");
      close (global_state.oxmem_info.sockfd);
      return 1;
    }

  // Initialize receive post-processing queue and start post-handler thread
  initQueue (&q_recv_post);
  sem_init (&recv_post_sem, 0, 0);
  pthread_t post_handler_tid;
  if (pthread_create (&post_handler_tid, NULL, post_handler_thread, NULL) !=
      0)
    {
      perror ("Failed to create post-handler thread");
      close (global_state.oxmem_info.sockfd);
      return 1;
    }

  // Start receive thread (fast path only)
  pthread_t recv_tid;
  if (pthread_create (&recv_tid, NULL, recv_thread, NULL) != 0)
    {
      perror ("Failed to create receive thread");
      close (global_state.oxmem_info.sockfd);
      return 1;
    }
  // Open connection (simplified - blocking)
  printf ("Establishing OmniXtend connection...\n");

  my_mac = get_mac_addr_from_devname (global_state.oxmem_info.sockfd, netdev);

  if (0 > send_open_connection (my_mac, global_state.oxmem_info.mac_addr))
    {
      LOG_ERROR ("Connection Failed.");
      return 0;
    }

  printf ("Connection established (ID: %d)\n",
	  global_state.oxmem_info.connection_id);

  // Initialize and start all NBD devices
  printf ("\nSetting up %d NBD device(s)...\n", num_devices);
  for (i = 0; i < num_devices; i++)
    {
      struct oxmem_bdev *bdev = &global_state.bdevs[i];

      // Initialize device-specific fields
      bdev->device_index = i;
      bdev->device_size = device_size;
      bdev->device_base = global_state.oxmem_info.base + (i * device_size);
      snprintf (bdev->device_path, sizeof (bdev->device_path),
		"/dev/nbd%d", i);

      printf ("  Device %d: %s (size: %zu bytes, base: 0x%lx)\n",
	      i, bdev->device_path, bdev->device_size, bdev->device_base);

      // Open NBD device
      bdev->nbd_fd = open (bdev->device_path, O_RDWR);
      if (bdev->nbd_fd < 0)
	{
	  fprintf (stderr, "Failed to open %s\n", bdev->device_path);
	  fprintf (stderr, "Hint: sudo modprobe nbd max_part=8\n");
	  cleanup_and_exit ();
	  return 1;
	}
      // Create socketpair
      if (socketpair (AF_UNIX, SOCK_STREAM, 0, bdev->sk_pair) < 0)
	{
	  perror ("Failed to create socketpair");
	  close (bdev->nbd_fd);
	  cleanup_and_exit ();
	  return 1;
	}
      // Configure NBD
      if (ioctl (bdev->nbd_fd, NBD_SET_SIZE, bdev->device_size) < 0)
	{
	  perror ("NBD_SET_SIZE failed");
	  cleanup_and_exit ();
	  return 1;
	}

      ioctl (bdev->nbd_fd, NBD_CLEAR_SOCK);
      ioctl (bdev->nbd_fd, NBD_SET_BLKSIZE, 4096);
      ioctl (bdev->nbd_fd, NBD_SET_TIMEOUT, 10);

      if (ioctl (bdev->nbd_fd, NBD_SET_SOCK, bdev->sk_pair[0]) < 0)
	{
	  perror ("NBD_SET_SOCK failed");
	  cleanup_and_exit ();
	  return 1;
	}
    }

  printf ("\nAll block devices ready\n");
  printf ("Press Ctrl+C to stop...\n");
  fflush (stdout);

  // Start NBD server threads (handle NBD protocol requests)
  for (i = 0; i < num_devices; i++)
    {
      if (pthread_create
	  (&nbd_threads[i], NULL, nbd_server_thread,
	   &global_state.bdevs[i]) != 0)
	{
	  fprintf (stderr,
		   "Failed to create NBD server thread for device %d\n", i);
	  cleanup_and_exit ();
	  return 1;
	}
    }

  usleep (100000);		// Small delay to let server threads start

  // Start NBD_DO_IT threads (one per device, each blocks until disconnect)
  for (i = 0; i < num_devices; i++)
    {
      if (pthread_create
	  (&nbd_do_it_threads[i], NULL, nbd_do_it_thread,
	   &global_state.bdevs[i]) != 0)
	{
	  fprintf (stderr,
		   "Failed to create NBD_DO_IT thread for device %d\n", i);
	  cleanup_and_exit ();
	  return 1;
	}
    }

  // Wait for all NBD_DO_IT threads (they block until disconnect)
  for (i = 0; i < num_devices; i++)
    {
      pthread_join (nbd_do_it_threads[i], NULL);
    }

  // Wait for all NBD server threads
  for (i = 0; i < num_devices; i++)
    {
      pthread_join (nbd_threads[i], NULL);
    }

  // Cleanup
  cleanup_and_exit ();
  return 0;
}
