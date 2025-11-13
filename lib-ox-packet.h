#ifndef __OMEM_H__
#define __OMEM_H__

#include <stdint.h>
#include <semaphore.h>

#define NUM_CONNECTION	4	// Maximum number of connection
#define MEM_SIZE		(8ULL * 1024 * 1024 * 1024)	// 8GB
#define BUFFER_SIZE	2048
#define MAX_FLIT_NUM	(BUFFER_SIZE/8)	//1 flit = 8 bytes


#define OX_START_ADDR	0x0
#define OX_ETHERTYPE	0xAAAA

#define NORMAL			0
#define ACK_ONLY		1
#define OPEN_CONN		2
#define CLOSE_CONN		3

#define NACK			0
#define ACK				1

#define CHANNEL_A		1
#define CHANNEL_B		2
#define CHANNEL_C		3
#define CHANNEL_D		4
#define CHANNEL_E		5

// Channel A
#define A_PUTFULLDATA_OPCODE	0
#define A_PUTPARTIALDATA_OPCODE	1
#define A_GET_OPCODE			4
#if 0
#define A_ARITHMETICDATA_OPCODE	2
#define A_LOGICALDATA_OPCODE	3
#define A_INTENT_OPCODE			5
#define A_ACQUIREBLOCK_OPCODE	6
#define A_ACQUIREPERM_OPCODE	7
#endif

// Channel B
#if 0
#define B_PUTFULLDATA_OPCODE	0
#define B_PUTPARTIALDATA_OPCODE	1
#define B_ARITHMETICDATA_OPCODE	2
#define B_LOGICALDATA_OPCODE	3
#define B_GET_OPCODE			4
#define B_INTENT_OPCODE			5
#define B_PROBEBLOCK_OPCODE		6
#define B_PROBEPERM_OPCODE		7
#endif

// Channel C
#if 0
#define C_ACCESSACK_OPCODE		0
#define C_ACCESSACKDATA_OPCODE	1
#define C_HINTACK_OPCODE		2
#define C_PROBEACK_OPCODE		4
#define C_PROBEACKDATA_OPCODE	5
#define C_RELEASE_OPCODE		6
#define C_RELEASEDATA_OPCODE	7
#endif

// Channel D
#define D_ACCESSACK_OPCODE		0
#define D_ACCESSACKDATA_OPCODE	1
#if 0
#define D_HINTACK_OPCODE		2
#define D_GRANT_OPCODE			4
#define D_GRANTDATA_OPCODE		5
#define D_RELEASEACK_OPCODE		6
#endif

// Channel E
#if 0
#define E_GRANTACK				0	// Not required
							// opcode
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

struct ox11_header {
    uint64_t dst_mac_addr:48;
    uint64_t src_mac_addr:48;
    unsigned short eth_type;
    uint64_t be64_tloe_header;
    uint64_t be64_tl_msg;
    uint64_t be64_tl_data_start;
} __attribute__((__packed__));

struct ox_connection {
    uint64_t my_mac_addr:48;
    uint64_t your_mac_addr:48;
    unsigned int my_seq_num:22;
    unsigned int your_seq_num_expected:22;
    unsigned char credit:5;
};

struct eth_header {
    uint64_t dst_mac_addr:48;
    uint64_t src_mac_addr:48;
    unsigned short eth_type;
} __attribute__((__packed__));

/*
 * TLoE Header in Host Endian (LE) 
 */
struct tloe_header {
    unsigned char credit:5;
    unsigned char chan:3;
    unsigned char reserve3:1;
    unsigned char ack:1;
    unsigned int seq_num_ack:22;
    unsigned int seq_num:22;
    unsigned char reserve2:2;
    unsigned char reserve1:1;
    unsigned char msg_type:4;
    unsigned char vc:3;
} __attribute__((packed, aligned(8)));

struct tl_msg_header_chan_AD {
    unsigned int source:26;
    unsigned int reserve1:12;
    unsigned int err:2;
    unsigned int domain:8;
    unsigned int size:4;
    unsigned int param:4;
    unsigned int reserve2:1;
    unsigned int opcode:3;
    unsigned int chan:3;
    unsigned int reserve3:1;
} __attribute__((packed, aligned(8)));

struct ox_packet_struct {
    struct eth_header eth_hdr;
    struct tloe_header tloe_hdr;
    uint64_t tl_msg_mask;
    uint32_t flit_cnt;
    uint64_t flits[MAX_FLIT_NUM];
//    uint64_t *flits;
};

struct oxmem_info_struct {
    int netdev_id;
    char netdev[64];
    int sockfd;
    struct stat st;
    uint64_t base;
    uint64_t size;
    uint64_t mac_addr;
    int connection_id;
};

enum ox_request_state {
    FREE = 0,
    POSTED = 1,
    SENT = 2,
    RESPONDED = 3
};

struct ox_request {
    int connection_id;
    int seq_num;
    struct ox_packet_struct send_ox;
    char send_buffer[BUFFER_SIZE];
    int send_size;
//    enum ox_request_state state;
    sem_t sem_wait;
    struct ox_packet_struct recv_ox;
    char recv_buffer[BUFFER_SIZE];
    int recv_size;
    char *target_buf;
    int expect_tl_msg; //1=tl_msg needed in response, 0=don't need tl_msg in response
//    int result; //1=Success, 0=Fail
};

#define FUSE_DIRECT_IO 1	// 1=mmap MAP_PRIVATE only, 0=mmap MAP_SHARED possible
#define OX_REQUEST_LIST_LENGTH 1
#define READ_WRITE_UNIT 1024	// max read/write data size for a request
#define DEFAULT_CREDIT 12	// 2^10 x 8byte

// Function declarations
int ox_struct_to_packet(struct ox_packet_struct *, char *, int *);
int packet_to_ox_struct(char *, int, struct ox_packet_struct *);
void make_response_packet_template(int connection_id,
				   struct ox_packet_struct *recv_ox_p,
				   struct ox_packet_struct *send_ox_p);
int get_connection(struct ox_packet_struct *);
int delete_connection(int);
int create_new_connection(struct ox_packet_struct *);
int make_open_connection_packet(int sockfd, char *netdev, uint64_t dst_mac,
				struct ox_packet_struct *send_ox_p);
int make_close_connection_packet(int connection_id,
				 struct ox_packet_struct *send_ox_p);
int make_get_op_packet(int connection_id, size_t size, off_t offset,
		       struct ox_packet_struct *send_ox_p);
int make_putfull_op_packet(int connection_id, const char *buf,
			   size_t size, off_t offset,
			   struct ox_packet_struct *send_ox_p);
int make_ack_packet(int connection_id, struct ox_packet_struct *recv_ox_p,
		    struct ox_packet_struct *send_ox_p);
int set_seq_num_to_ox_packet(int connection_id,
			     struct ox_packet_struct *send_ox_p);
int update_seq_num_expected(int connection_id,
			    struct ox_packet_struct *recv_ox_p);
int get_seq_num_expected(int connection_id);

#endif				/* __OMEM_H__ */
