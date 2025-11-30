#include <stdio.h>
#include <endian.h>
#include <string.h>
#include <netinet/in.h>
#include <net/if.h>
#include <linux/if_ether.h>
#include <sys/ioctl.h>
#include <pthread.h>

#include "lib-ox-packet.h"

static struct ox_connection ox_conn_list[NUM_CONNECTION];

/**
 * @brief Convert an omnixtend structure to packet format
 */
int
//ox_struct_to_packet(struct ox_packet_struct *ox_p, char *send_buffer,
//		    int *send_buffer_size)
ox_struct_to_packet(struct ox_packet_struct *ox_p)
{
    int packet_size = 0;
    uint64_t mask;
    uint64_t be64_temp;
    int offset = 0;
    char *buffer = ox_p->packet_buffer;

    if (ox_p->flit_cnt < 5) {	// at minimum. packet_size must be 70
	packet_size +=
	    (sizeof(struct eth_header) + sizeof(struct tloe_header) +
	     (sizeof(uint64_t) * 5) + sizeof(ox_p->tl_msg_mask));
    } else {
	packet_size +=
	    (sizeof(struct eth_header) + sizeof(struct tloe_header) +
	     (sizeof(uint64_t) * ox_p->flit_cnt) +
	     sizeof(ox_p->tl_msg_mask));
    }

    bzero((void *) buffer, sizeof(struct eth_header) + sizeof(struct tloe_header));

    // Ethernet Header
    memcpy(buffer, &(ox_p->eth_hdr), sizeof(struct eth_header));
    offset += sizeof(struct eth_header);

    // TLoE frame Header
    be64_temp = htobe64(*(uint64_t *) & (ox_p->tloe_hdr));
    memcpy(buffer + offset, &be64_temp, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    if ( ox_p->flits == NULL)
        ox_p->flits = (uint64_t *)(buffer + offset);

    // TLoE frame mask
    mask = htobe64(ox_p->tl_msg_mask);
    memcpy(buffer + packet_size - sizeof(uint64_t), &mask,
	   sizeof(uint64_t));

    ox_p->packet_size = packet_size;

    return 0;
}

/**
 * @brief Change the packet to an omnixtend structure
 */
int
//packet_to_ox_struct(char *recv_buffer, int recv_size,
//		    struct ox_packet_struct *ox_p)
packet_to_ox_struct(struct ox_packet_struct *ox_p)
{
    uint64_t tl_msg_mask = 0;
    uint64_t tloe_hdr = 0;
    int tl_msg_full_count_by_8bytes = 0;
    struct eth_header *recv_packet_eth_hdr;
    struct tloe_header *recv_packet_tloe_hdr;
    int packet_size = ox_p->packet_size;

    char *buffer = ox_p->packet_buffer;

    struct tl_msg_header_chan_AD __tl_msg_hdr = { 0, };
    uint64_t temp_tl_msg_hdr =
	*(uint64_t *) (buffer + sizeof(struct eth_header) +
		       sizeof(struct tloe_header));
    *(uint64_t *) & __tl_msg_hdr = be64toh(temp_tl_msg_hdr);

    // Ethernet MAC header (14 bytes)
    recv_packet_eth_hdr = (struct eth_header *) buffer;

    memcpy(&(ox_p->eth_hdr), recv_packet_eth_hdr,
	   sizeof(struct eth_header));

    // TLoE frame header (8 bytes)
    recv_packet_tloe_hdr =
	(struct tloe_header *) (buffer + sizeof(struct eth_header));
    tloe_hdr = be64toh(*(uint64_t *) recv_packet_tloe_hdr);
    memcpy(&(ox_p->tloe_hdr), &tloe_hdr, sizeof(uint64_t));

    // TileLink messages (8 bytes * n)
    tl_msg_full_count_by_8bytes =
	(packet_size - sizeof(struct eth_header) -
	 sizeof(struct tloe_header) -
	 sizeof(uint64_t) /* mask */ ) / sizeof(uint64_t);
    ox_p->flit_cnt = tl_msg_full_count_by_8bytes;

    // just pass the pointer of receive buffer
    ox_p->flits =
	(uint64_t *) (buffer + sizeof(struct eth_header) +
		      sizeof(struct tloe_header));

    // TLoE frame mask (8 bytes)
    memcpy(&tl_msg_mask, buffer + packet_size - sizeof(tl_msg_mask),
	   sizeof(tl_msg_mask));
    ox_p->tl_msg_mask = be64toh(tl_msg_mask);

    return 0;
}

int
add_new_connection(uint64_t your_mac_addr, uint64_t my_mac_addr,
		   uint32_t initial_seq_num)
{
    int i;

    // check if same mac is already registered, overwrite it.
    for (i = 0; i < NUM_CONNECTION; i++) {
	if (ox_conn_list[i].your_mac_addr == your_mac_addr) {
	    ox_conn_list[i].my_mac_addr = my_mac_addr;
	    ox_conn_list[i].my_seq_num = 0;
	    ox_conn_list[i].your_seq_num_expected = initial_seq_num + 1;
	    ox_conn_list[i].credit = 28;
	    return i;
	}
    }

    if (i == NUM_CONNECTION) {	// If there is no match, find an empty
	// slot and create new connection
	for (i = 0; i < NUM_CONNECTION; i++) {
	    if (ox_conn_list[i].your_mac_addr == 0) {
		ox_conn_list[i].your_mac_addr = your_mac_addr;
		ox_conn_list[i].my_mac_addr = my_mac_addr;
		ox_conn_list[i].my_seq_num = 0;
		ox_conn_list[i].your_seq_num_expected =
		    initial_seq_num + 1;
		ox_conn_list[i].credit = 28;
		// If creation success, return index.
		return i;
	    }
	}
    }
    // If there is no empty slot, return error.
    return -1;
}

/**
 * @brief Delete connection ID with a matching source mac address
 * @return 0 : success
 */
int delete_connection(int connection_id)
{
    ox_conn_list[connection_id].your_mac_addr = 0;

    return 0;
}

void setup_send_ox_eth_hdr(int connection_id, struct ox_packet_struct * send_ox_p)
{
    if ( connection_id < 0 ) return;

//    memset(send_ox_p, 0, sizeof(struct ox_packet_struct));

    send_ox_p->eth_hdr.dst_mac_addr = ox_conn_list[connection_id].your_mac_addr;
    send_ox_p->eth_hdr.src_mac_addr = ox_conn_list[connection_id].my_mac_addr;
    send_ox_p->eth_hdr.eth_type = OX_ETHERTYPE;
}

// get mac addr of netdev
uint64_t get_mac_addr_from_devname(int sockfd, char *netdev)
{
    struct ifreq s;
    unsigned char *mac = NULL;
    uint64_t my_mac = 0;
    int i;

    strcpy(s.ifr_name, netdev);

    if (0 == ioctl(sockfd, SIOCGIFHWADDR, &s)) {
	mac = (unsigned char *) s.ifr_hwaddr.sa_data;
	for (i = 0; i < 6; i++) {
	    my_mac += ((uint64_t) mac[i]) << (i * 8);
	}
	PRINT_LINE("netdev=%s mac = %lx\n", netdev, my_mac);
    }

    return my_mac;
}

static pthread_mutex_t seq_num_lock = PTHREAD_MUTEX_INITIALIZER;

int set_seq_num_to_ox_packet(int connection_id, struct ox_packet_struct *send_ox_p)
{
	pthread_mutex_lock(&seq_num_lock);

	send_ox_p->tloe_hdr.seq_num = ox_conn_list[connection_id].my_seq_num++;
	send_ox_p->tloe_hdr.seq_num_ack = ox_conn_list[connection_id].your_seq_num_expected-1;

	pthread_mutex_unlock(&seq_num_lock);

	return 0;
}

#define SEQ_NUM_VALID_WINDOW 1000

int update_seq_num_expected(int connection_id, struct ox_packet_struct *recv_ox_p)
{
    int ret = 0;

	PRINT_LINE("recv seq_num  = %x\n", recv_ox_p->tloe_hdr.seq_num);
	pthread_mutex_lock(&seq_num_lock);
/*	if ( ((recv_ox_p->tloe_hdr.seq_num >= ox_conn_list[connection_id].your_seq_num_expected) &&
        SEQ_NUM_VALID_WINDOW > (recv_ox_p->tloe_hdr.seq_num - ox_conn_list[connection_id].your_seq_num_expected)) || ((recv_ox_p->tloe_hdr.seq_num < ox_conn_list[connection_id].your_seq_num_expected ) && SEQ_NUM_VALID_WINDOW > ( 0x400000 - (ox_conn_list[connection_id].your_seq_num_expected - recv_ox_p->tloe_hdr.seq_num))) ) {
	    ox_conn_list[connection_id].your_seq_num_expected = recv_ox_p->tloe_hdr.seq_num + 1;
        ret = 1;
    } else if ( recv_ox_p->tloe_hdr.seq_num == (ox_conn_list[connection_id].your_seq_num_expected-1)) {
        ret = 0; //same seq with before seq
	} else
        ret = -1;
*/
    if ( recv_ox_p->tloe_hdr.seq_num >= ox_conn_list[connection_id].your_seq_num_expected )
        ox_conn_list[connection_id].your_seq_num_expected = recv_ox_p->tloe_hdr.seq_num + 1;

	pthread_mutex_unlock(&seq_num_lock);

	return ret;
}

int get_seq_num_expected(int connection_id)
{
	return ox_conn_list[connection_id].your_seq_num_expected;
}




#if 0
/**
 * @brief Print payload in Hex format
 */
void print_payload(char *data, int size)
{
    int i, j;

    for (i = 0; i < size; i++) {
	if (i != 0 && i % 16 == 0) {
	    printf("\t");
	    for (j = i - 16; j < i; j++) {
		if (data[j] >= 32 && data[j] < 128)
		    PRINT_LINE("%c", (unsigned char) data[j]);
		else
		    PRINT_LINE(".");
	    }
	    PRINT_LINE("\n");
	}

	if ((i % 8) == 0 && (i % 16) != 0)
	    PRINT_LINE(" ");
	PRINT_LINE(" %02X", (unsigned char) data[i]);	// print DATA

	if (i == size - 1) {
	    for (j = 0; j < (15 - (i % 16)); j++)
		PRINT_LINE("   ");

	    PRINT_LINE("\t");

	    for (j = (i - (i % 16)); j <= i; j++) {
		if (data[j] >= 32 && data[j] < 128)
		    PRINT_LINE("%c", (unsigned char) data[j]);
		else
		    PRINT_LINE(".");
	    }
	    PRINT_LINE("\n");
	}
    }
}

/**
 * @brief Return connection ID with a matching source mac address 
 * @param ox_packet_structure
 * @return conntction id
 */
int get_connection(struct ox_packet_struct *ox_p)
{
    int i;

    if (!ox_p)
	return -1;

    for (i = 0; i < NUM_CONNECTION; i++) {
	if (ox_conn_list[i].your_mac_addr == ox_p->eth_hdr.src_mac_addr) {
	    if (ox_conn_list[i].your_seq_num_expected <=
		ox_p->tloe_hdr.seq_num)
		// update seq_num_expected.
		ox_conn_list[i].your_seq_num_expected =
		    ox_p->tloe_hdr.seq_num + 1;
	    return i;
	}
    }
    printf("Connection Error! - MAC %lx is not found in list.\n",
	   be64toh(ox_p->eth_hdr.src_mac_addr) >> 16);

    return -1;
}

/**
 * @brief Create new connection for msg type 2(Open_connection)
 * @return Index num., -1 : error
 */
int
add_new_connection(uint64_t your_mac_addr, uint64_t my_mac_addr,
		   uint32_t initial_seq_num)
{
    int i;

    // check if same mac is already registered, overwrite it.
    for (i = 0; i < NUM_CONNECTION; i++) {
	if (ox_conn_list[i].your_mac_addr == your_mac_addr) {
	    ox_conn_list[i].my_mac_addr = my_mac_addr;
	    ox_conn_list[i].my_seq_num = 0;
	    ox_conn_list[i].your_seq_num_expected = initial_seq_num + 1;
	    ox_conn_list[i].credit = 28;
	    return i;
	}
    }

    if (i == NUM_CONNECTION) {	// If there is no match, find an empty
	// slot and create new connection
	for (i = 0; i < NUM_CONNECTION; i++) {
	    if (ox_conn_list[i].your_mac_addr == 0) {
		ox_conn_list[i].your_mac_addr = your_mac_addr;
		ox_conn_list[i].my_mac_addr = my_mac_addr;
		ox_conn_list[i].my_seq_num = 0;
		ox_conn_list[i].your_seq_num_expected =
		    initial_seq_num + 1;
		ox_conn_list[i].credit = 28;
		// If creation success, return index.
		return i;
	    }
	}
    }
    // If there is no empty slot, return error.
    return -1;
}

/*
int atomic_get_seq_num(int connection_id)
{
	int seq_num;
	seq_num = ox_conn_list[connection_id].my_seq_num;
	PRINT_LINE("connection_id=%d seq_num=%d\n", connection_id, seq_num);
	ox_conn_list[connection_id].my_seq_num++;
	return seq_num;
}
*/
void
make_response_packet_template(int connection_id,
			      struct ox_packet_struct *recv_ox_p,
			      struct ox_packet_struct *send_ox_p)
{
    send_ox_p->eth_hdr.dst_mac_addr = recv_ox_p->eth_hdr.src_mac_addr;
    send_ox_p->eth_hdr.src_mac_addr = recv_ox_p->eth_hdr.dst_mac_addr;
    send_ox_p->eth_hdr.eth_type = recv_ox_p->eth_hdr.eth_type;

    send_ox_p->tloe_hdr.chan = recv_ox_p->tloe_hdr.chan;
    send_ox_p->tloe_hdr.seq_num_ack = recv_ox_p->tloe_hdr.seq_num;
    send_ox_p->tloe_hdr.credit = ox_conn_list[connection_id].credit;
//    send_ox_p->tloe_hdr.seq_num = ox_conn_list[connection_id].my_seq_num++;
    send_ox_p->tloe_hdr.ack = 1;
    send_ox_p->tloe_hdr.msg_type = NORMAL;	// Normal type
    send_ox_p->tloe_hdr.vc = recv_ox_p->tloe_hdr.vc;

}


/**
 * @brief
 */
int
make_open_connection_packet(int sockfd, char *netdev, uint64_t dst_mac,
			    struct ox_packet_struct *send_ox_p)
{
    char recv_buffer[BUFFER_SIZE] = { 0, };
    // int send_buffer_size = 0;
    struct ox_packet_struct recv_ox_p	/* this is made by hand, not
					 * received */ ;
    uint64_t my_mac;
    int connection_id;
    int recv_size;
    int ret = 0;
    int msg_type;

    my_mac = get_mac_addr_from_devname(sockfd, netdev);
    PRINT_LINE("netdev=%s my_mac=%lx dst_mac = %lx\n", netdev, my_mac,
	      dst_mac);

    bzero(send_ox_p, sizeof(struct ox_packet_struct));
    bzero(&recv_ox_p, sizeof(struct ox_packet_struct));

    if (connection_id = add_new_connection(dst_mac, my_mac, 0x3FFFFF) < 0) {
	// No slot
	PRINT_LINE("Error - There is no room for new connection.\n");
	return -1;
    }
    // make recv_ox_p from scratch
    recv_ox_p.eth_hdr.src_mac_addr = dst_mac;
    recv_ox_p.eth_hdr.dst_mac_addr = my_mac;
    recv_ox_p.eth_hdr.eth_type = OX_ETHERTYPE;
    recv_ox_p.tloe_hdr.chan = CHANNEL_A;
    recv_ox_p.tloe_hdr.seq_num = 0x3FFFFF;
    recv_ox_p.tloe_hdr.vc = 0;

    make_response_packet_template(connection_id, &recv_ox_p, send_ox_p);
    send_ox_p->tloe_hdr.msg_type = OPEN_CONN;
    send_ox_p->tloe_hdr.chan = CHANNEL_A;
    send_ox_p->tloe_hdr.ack = 0;	// This is not ACK. it's 1st
    // packet to make a connection.

    return connection_id;

}

/**
 * @brief
 */
int
make_close_connection_packet(int connection_id,
			     struct ox_packet_struct *send_ox_p)
{
    struct ox_packet_struct recv_ox_p	/* this is made by hand, not
					 * received */ ;
    bzero(send_ox_p, sizeof(struct ox_packet_struct));

    send_ox_p->eth_hdr.dst_mac_addr =
	ox_conn_list[connection_id].your_mac_addr;
    send_ox_p->eth_hdr.src_mac_addr =
	ox_conn_list[connection_id].my_mac_addr;
    send_ox_p->eth_hdr.eth_type = OX_ETHERTYPE;

    send_ox_p->tloe_hdr.chan = 0;
    send_ox_p->tloe_hdr.seq_num_ack =
	ox_conn_list[connection_id].your_seq_num_expected - 1;
    send_ox_p->tloe_hdr.credit = 0;
//    send_ox_p->tloe_hdr.seq_num = ox_conn_list[connection_id].my_seq_num++;
    send_ox_p->tloe_hdr.ack = 1;
    send_ox_p->tloe_hdr.msg_type = CLOSE_CONN;	// Close Connection
    send_ox_p->tloe_hdr.vc = 0;

    return 0;

}

/**
 * @brief
 */
int make_ack_packet(int connection_id,
			     struct ox_packet_struct *recv_ox_p, struct ox_packet_struct *send_ox_p)
{
    send_ox_p->eth_hdr.dst_mac_addr =
	ox_conn_list[connection_id].your_mac_addr;
    send_ox_p->eth_hdr.src_mac_addr =
	ox_conn_list[connection_id].my_mac_addr;
    send_ox_p->eth_hdr.eth_type = OX_ETHERTYPE;

    send_ox_p->tloe_hdr.chan = CHANNEL_D;	//ack for channel D packet
    send_ox_p->tloe_hdr.seq_num_ack = 
	ox_conn_list[connection_id].your_seq_num_expected - 1;
    send_ox_p->tloe_hdr.credit = 0;
//    send_ox_p->tloe_hdr.seq_num = ox_conn_list[connection_id].my_seq_num++;
    send_ox_p->tloe_hdr.ack = 1;
    send_ox_p->tloe_hdr.msg_type = NORMAL;	// Normal type
    send_ox_p->tloe_hdr.vc = 0;

    return 0;

}



/**
 * @brief
 * size must be 2^n (n<=10)
 */
int
make_get_op_packet(int connection_id, size_t size, off_t offset,
		   struct ox_packet_struct *send_ox_p,
		   uint64_t * send_flits)
{
    struct tl_msg_header_chan_AD tl_msg_header;
    uint64_t be64_temp;
    int size_bit = 0;

    for (size_bit = 0; size_bit < 16; size_bit++) {
	if (size == 1 << size_bit)
	    break;
    }

    if (size_bit > 10) {
	PRINT_LINE("size=%lu is not supported. size must be 2^n (n=3~10)\n",
		  size);
	return -1;
    }

    bzero(&tl_msg_header, sizeof(struct tl_msg_header_chan_AD));
    bzero(send_ox_p, sizeof(struct ox_packet_struct));
    send_ox_p->flits = send_flits;

    // Make a tilelink message and add as a flit
    tl_msg_header.source = 0;
    tl_msg_header.size = size_bit;
    tl_msg_header.opcode = A_GET_OPCODE;
    tl_msg_header.chan = CHANNEL_A;

    be64_temp = htobe64(*(uint64_t *) & (tl_msg_header));
    send_ox_p->flits[send_ox_p->flit_cnt] = be64_temp;
    send_ox_p->tl_msg_mask |= 1 << send_ox_p->flit_cnt;
    send_ox_p->flit_cnt += 1;
    send_ox_p->flits[send_ox_p->flit_cnt] = htobe64(offset);	// address
    send_ox_p->flit_cnt += 1;

    send_ox_p->eth_hdr.dst_mac_addr =
	ox_conn_list[connection_id].your_mac_addr;
    send_ox_p->eth_hdr.src_mac_addr =
	ox_conn_list[connection_id].my_mac_addr;
    send_ox_p->eth_hdr.eth_type = OX_ETHERTYPE;

    send_ox_p->tloe_hdr.chan = 0;
    send_ox_p->tloe_hdr.seq_num_ack =
	ox_conn_list[connection_id].your_seq_num_expected - 1;
    send_ox_p->tloe_hdr.credit = 0;
//    send_ox_p->tloe_hdr.seq_num = ox_conn_list[connection_id].my_seq_num++;
    send_ox_p->tloe_hdr.ack = 1;
    send_ox_p->tloe_hdr.msg_type = NORMAL;
    send_ox_p->tloe_hdr.vc = 0;

    return 0;

}

/**
 * @brief
 * size must be 2^n (n=0~10)
 */
int
make_putfull_op_packet(int connection_id, const char *buf, size_t size,
		       off_t offset, struct ox_packet_struct *send_ox_p,
		       uint64_t * send_flits)
{
    struct tl_msg_header_chan_AD tl_msg_header;
    uint64_t be64_temp;
    int size_bit = 0;

    for (size_bit = 0; size_bit < 16; size_bit++) {
	if (size == 1 << size_bit)
	    break;
    }

    if (size_bit > 10) {
	PRINT_LINE("size=%lu is not supported. size must be 2^n (n=0~10)\n",
		  size);
	return -1;
    }

    bzero(&tl_msg_header, sizeof(struct tl_msg_header_chan_AD));
    bzero(send_ox_p, sizeof(struct ox_packet_struct));
    send_ox_p->flits = send_flits;

    // Make a tilelink message and add as a flit
    tl_msg_header.source = 0;
    tl_msg_header.size = size_bit;
    tl_msg_header.opcode = A_PUTFULLDATA_OPCODE;
    tl_msg_header.chan = CHANNEL_A;

    be64_temp = htobe64(*(uint64_t *) & (tl_msg_header));
    send_ox_p->flits[send_ox_p->flit_cnt] = be64_temp;
    send_ox_p->tl_msg_mask |= 1 << send_ox_p->flit_cnt;
    send_ox_p->flit_cnt += 1;
    send_ox_p->flits[send_ox_p->flit_cnt] = htobe64(offset);	// address
    send_ox_p->flit_cnt += 1;

    memcpy(&send_ox_p->flits[send_ox_p->flit_cnt], buf, size);
    send_ox_p->flit_cnt += size / sizeof(uint64_t);

    send_ox_p->eth_hdr.dst_mac_addr =
	ox_conn_list[connection_id].your_mac_addr;
    send_ox_p->eth_hdr.src_mac_addr =
	ox_conn_list[connection_id].my_mac_addr;
    send_ox_p->eth_hdr.eth_type = OX_ETHERTYPE;

    send_ox_p->tloe_hdr.chan = 0;
    send_ox_p->tloe_hdr.seq_num_ack =
	ox_conn_list[connection_id].your_seq_num_expected - 1;
    send_ox_p->tloe_hdr.credit = 0;
//    send_ox_p->tloe_hdr.seq_num = ox_conn_list[connection_id].my_seq_num++;
    send_ox_p->tloe_hdr.ack = 1;
    send_ox_p->tloe_hdr.msg_type = NORMAL;
    send_ox_p->tloe_hdr.vc = 0;

    return 0;

}

#endif

