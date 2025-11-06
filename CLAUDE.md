# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

oxmem-nbd is a userspace NBD (Network Block Device) driver that exposes OmniXtend Memory as a Linux block device. It implements the TileLink-over-Ethernet (TLoE) protocol to communicate with remote OmniXtend memory devices over raw Ethernet.

## Build Commands

```bash
# Build the driver (default target)
make

# Build just oxmem-nbd
make oxmem-nbd

# Build utility for creating test files
make make-increment-file

# Clean build artifacts
make clean
```

The build automatically sets `cap_net_raw+ep` capability on the binary (requires sudo) since raw socket access is needed for Ethernet communication.

## Running and Testing

```bash
# Load NBD kernel module (required once per boot)
sudo modprobe nbd max_part=8

# Run the driver (adjust parameters for your setup)
sudo ./oxmem-nbd \
  --netdev=enp179s0f0np0 \
  --mac="04:3f:72:dd:0b:05" \
  --size=8G \
  --base=0x0

# In another terminal, use the block device
sudo mkfs.ext4 /dev/nbd0
sudo mount /dev/nbd0 /mnt/oxmem

# Stop gracefully with Ctrl+C or kill
```

Required parameters: `--netdev`, `--mac`, `--size`
Optional parameters: `--base` (default: 0x0), `--device` (default: /dev/nbd0)

## Architecture

### Threading Model

The driver uses a simplified single-threaded blocking I/O model:

- **Main Thread**: Sets up NBD device, opens OmniXtend connection, then blocks in `NBD_DO_IT` ioctl waiting for disconnect
- **NBD Server Thread**: Single thread that handles all NBD protocol requests in a blocking manner (read request → send OmniXtend packet → wait for response → send reply)

This is significantly simpler than queue-based async approaches (~600 lines vs 900+).

### Communication Flow

```
/dev/nbd0 ←→ NBD Kernel Driver ←[socketpair]→ nbd_server_thread()
                                                       ↓
                                          oxmem_read_blocking() / oxmem_write_blocking()
                                                       ↓
                                          send_and_wait_response()
                                                       ↓
                                          Raw Ethernet Socket → OmniXtend Device
```

The socketpair created in `main()` connects the NBD kernel driver to the userspace NBD server thread. All block I/O flows through this socket.

### Key Data Structures

- `struct oxmem_bdev` (oxmem-nbd.c:32): Global state containing NBD fd, socketpair, network info, and credit tracking
- `struct ox_packet_struct` (lib-ox-packet.h:137): Represents a single OmniXtend packet with Ethernet header, TLoE header, TileLink message, and data flits
- `struct oxmem_info_struct` (lib-ox-packet.h:145): Network interface, socket, MAC address, memory base/size, connection ID
- `struct ox_connection` (lib-ox-packet.h:94): Per-connection state including MAC addresses, sequence numbers, and credits

## Protocol Implementation

### OmniXtend/TileLink Protocol

The code implements TileLink-over-Ethernet (TLoE) with these key components:

1. **Ethernet Layer**: Raw sockets with custom EtherType `0xAAAA`
2. **TLoE Header**: Contains channel, sequence numbers, ACK bits, and flow control credits (lib-ox-packet.h:111)
3. **TileLink Messages**: Channel A (GET, PUTFULLDATA) and Channel D (ACCESSACK, ACCESSACKDATA) for read/write operations
4. **Connection Management**: Open/close connection packets with sequence number tracking

### Flow Control

The driver implements a credit-based flow control system:
- `credit_in_use[]` array tracks outstanding credits per channel (5 channels: A-E)
- `get_ack_credit()` (oxmem-nbd.c:68): Calculates credit to acknowledge based on power-of-2 rounding
- `get_used_credit()` (oxmem-nbd.c:91): Extracts credit usage from received packets
- Maximum request size: 1024 bytes (`READ_WRITE_UNIT`), auto-chunked for larger I/O

### Request/Response Handling

All operations are blocking and synchronous:
- `send_and_wait_response()` (oxmem-nbd.c:129): Core function that sends a packet and polls for response with timeout
- Uses `select()` with `RESPONSE_TIMEOUT_SEC` (10 seconds) to wait for responses
- Automatically sends ACK packets when credits are consumed

## File Organization

- `oxmem-nbd.c`: Main driver, NBD protocol handling, blocking I/O operations
- `lib-ox-packet.c`: OmniXtend packet encoding/decoding, connection management, TLoE protocol
- `lib-ox-packet.h`: Protocol structures, constants, function declarations
- `lib-queue.c/h`: Simple linked-list queue (currently not heavily used in this simplified version)
- `make-increment-file.c`: Utility to create test files with incrementing byte pattern
- `Makefile`: Build configuration with gcc flags and capability setting

## Important Constants

- `NBD_DEVICE`: Default `/dev/nbd0`
- `READ_WRITE_UNIT`: 1024 bytes (max single request size)
- `RESPONSE_TIMEOUT_SEC`: 10 seconds
- `OX_ETHERTYPE`: 0xAAAA
- `BUFFER_SIZE`: 2048 bytes
- `NUM_CONNECTION`: 4 (max concurrent connections supported)

## Signal Handling

The driver handles SIGINT/SIGTERM gracefully:
1. Sets `shutdown_requested` flag
2. Sends NBD_DISCONNECT ioctl
3. Sends close connection packet to OmniXtend device
4. Cleans up NBD device (NBD_CLEAR_QUE, NBD_CLEAR_SOCK)
5. Closes sockets and releases resources

See `signal_handler()` and `cleanup_and_exit()` in oxmem-nbd.c:59 and oxmem-nbd.c:455.

## NBD Protocol Commands Supported

- `NBD_CMD_READ`: Translates to TileLink GET operation (Channel A → Channel D)
- `NBD_CMD_WRITE`: Translates to TileLink PUTFULLDATA operation (Channel A → Channel D)
- `NBD_CMD_FLUSH`: No-op (immediate success)
- `NBD_CMD_DISC`: Graceful disconnect

Block size is fixed at 4096 bytes as configured in the NBD_SET_BLKSIZE ioctl.

## Debugging

The code includes conditional debug macros:
- `PRINT_LINE()`: Disabled by default (lib-ox-packet.h:184)
- `PRINT_LINE1()`: Enabled by default (lib-ox-packet.h:190)

To enable detailed debugging, change the `#if 0` to `#if 1` in lib-ox-packet.h:184.

## Common Issues

- **"Failed to open NBD device"**: NBD kernel module not loaded (`sudo modprobe nbd`)
- **"Device or resource busy"**: Another process using /dev/nbd0 (check with `sudo nbd-client -c /dev/nbd0`)
- **"Permission denied" for raw socket**: Binary missing CAP_NET_RAW capability (rebuild with `make`)
- **Timeout errors**: Check network connectivity to OmniXtend device, verify MAC address
- **Connection refused**: Ensure target MAC address is correct and device is reachable
