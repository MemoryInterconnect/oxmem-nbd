# OmniXtend Memory Block Device

A simple userspace block device driver for OmniXtend Memory using the Linux NBD (Network Block Device) protocol.

## Quick Start

```bash
# Build
make oxmem-bdev

# Load NBD kernel module
sudo modprobe nbd max_part=8

# Run the driver (adjust parameters for your setup)
sudo ./oxmem-bdev \
  --netdev=enp179s0f0np0 \
  --mac="04:3f:72:dd:0b:05" \
  --size=8G \
  --base=0x0

# In another terminal, use the block device
sudo mkfs.ext4 /dev/nbd0
sudo mkdir -p /mnt/oxmem
sudo mount /dev/nbd0 /mnt/oxmem
cd /mnt/oxmem
# ... use as normal filesystem ...
```

## Why Block Device Instead of FUSE?

**Advantages:**
- **Simpler**: ~600 lines vs 900+ lines for FUSE implementations
- **Standard interface**: Works with all block device tools (mkfs, mount, fsck, etc.)
- **Any filesystem**: Format with ext4, xfs, btrfs, or any other filesystem
- **Better integration**: Proper block device semantics, buffering handled by kernel
- **No FUSE overhead**: Direct block I/O path

**Use Cases:**
- Regular file storage (ext4/xfs filesystem on top)
- Swap space (`mkswap /dev/nbd0`)
- LVM physical volume
- RAID member device
- Any use case requiring a standard block device

## Architecture

```
┌─────────────────────────────────────┐
│      User Application               │
│  (reads/writes to /mnt/oxmem)       │
└─────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│      Filesystem (ext4, xfs, etc)    │
└─────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│      NBD Kernel Driver              │
│      (/dev/nbd0)                    │
└─────────────────────────────────────┘
              │ (socket)
              ▼
┌─────────────────────────────────────┐
│   oxmem-bdev (userspace)            │
│   ┌───────────────────────────┐     │
│   │  NBD Server Thread        │     │
│   │  (handles NBD protocol)   │     │
│   └───────────────────────────┘     │
│              │                       │
│              ▼                       │
│   ┌───────────────────────────┐     │
│   │  OmniXtend Network Layer  │     │
│   │  - Send Thread            │     │
│   │  - Recv Thread            │     │
│   │  - Request Queue          │     │
│   └───────────────────────────┘     │
└─────────────────────────────────────┘
              │ (raw ethernet)
              ▼
┌─────────────────────────────────────┐
│   OmniXtend Memory Device           │
│   (remote memory over ethernet)     │
└─────────────────────────────────────┘
```

## Implementation Details

### NBD Protocol
- Handles NBD_CMD_READ, NBD_CMD_WRITE, NBD_CMD_FLUSH, NBD_CMD_DISC
- Uses socketpair() for communication between kernel and userspace
- Block size: 4096 bytes

### Network Layer
- Same OmniXtend protocol as FUSE implementations
- Raw Ethernet sockets (requires CAP_NET_RAW)
- TileLink over Ethernet (TLoE) protocol
- Channels A and D for read/write operations
- Maximum request size: 1024 bytes (auto-chunked)

### Threading
- **NBD Thread**: Handles NBD protocol requests from kernel
- **Send Thread**: Sends OmniXtend packets to network
- **Recv Thread**: Receives OmniXtend responses from network
- **Main Thread**: Setup and NBD_DO_IT ioctl (blocks until disconnect)

## Command Line Options

**Required:**
- `--netdev=DEV` : Network interface name (e.g., eth0)
- `--mac=MAC` : MAC address of OmniXtend endpoint (e.g., aa:bb:cc:dd:ee:ff)
- `--size=SIZE` : Size with unit (e.g., 4G, 1024M, 2048K)

**Optional:**
- `--base=ADDR` : Base address in hex (default: 0x0)
- `--device=DEV` : NBD device path (default: /dev/nbd0)
- `-h, --help` : Show help

## Examples

### Basic Usage
```bash
# 4GB device
sudo ./oxmem-bdev --netdev=eth0 --mac=aa:bb:cc:dd:ee:ff --size=4G
```

### Create ext4 Filesystem
```bash
sudo mkfs.ext4 /dev/nbd0
sudo mount /dev/nbd0 /mnt
echo "Hello OmniXtend" > /mnt/test.txt
cat /mnt/test.txt
sudo umount /mnt
```

### Use as Swap
```bash
sudo mkswap /dev/nbd0
sudo swapon /dev/nbd0
# ... system uses it as swap ...
sudo swapoff /dev/nbd0
```

### Check Device Status
```bash
# While driver is running
lsblk /dev/nbd0
sudo blockdev --getsize64 /dev/nbd0
sudo fdisk -l /dev/nbd0
```

### Read/Write Tests
```bash
# Direct block device access requires sudo
# Read 4KB from device
sudo dd if=/dev/nbd0 of=./testdat_4k.dat bs=4k count=1

# Write 4KB to device
sudo dd if=./testdat_4k.dat of=/dev/nbd0 bs=4k count=1

# Access via mounted filesystem (no sudo needed after mounting)
sudo mkfs.ext4 /dev/nbd0
sudo mount /dev/nbd0 /mnt
echo "test" > /mnt/testfile.txt
cat /mnt/testfile.txt
sudo umount /mnt
```

## Stopping the Driver

Simply press Ctrl+C in the terminal running oxmem-bdev. It will:
1. Handle the signal gracefully (SIGINT/SIGTERM)
2. Send disconnect packet to OmniXtend device
3. Wait for disconnect confirmation (2-second timeout)
4. Clean up NBD device (NBD_CLEAR_QUE, NBD_CLEAR_SOCK)
5. Release all resources (sockets, threads, memory)
6. Exit cleanly

Example output:
```
^C
Closing OmniXtend connection...
Disconnect packet sent successfully
Cleanup complete
```

You can also terminate from another terminal:
```bash
kill <pid>          # Sends SIGTERM
kill -INT <pid>     # Sends SIGINT (same as Ctrl+C)
```

See `SIGNAL-HANDLING.md` for detailed information about signal handling.

## Troubleshooting

### "Failed to open NBD device"
```bash
# Make sure NBD module is loaded
sudo modprobe nbd
lsmod | grep nbd
```

### "Device or resource busy"
```bash
# Make sure no other process is using the NBD device
sudo nbd-client -d /dev/nbd0
# or
sudo nbd-client -c /dev/nbd0  # check status
```

### "Permission denied" for raw socket
```bash
# Check capabilities
getcap ./oxmem-bdev
# Should show: cap_net_raw+ep

# If not, rebuild:
make oxmem-bdev
```

### "Permission denied" when accessing /dev/nbd0
```bash
# Block devices require root access or disk group membership

# Solution 1: Use sudo (recommended)
sudo dd if=/dev/nbd0 of=./testdat_4k.dat bs=4k count=1

# Solution 2: Add user to disk group (security risk - understand implications)
sudo usermod -a -G disk $USER
# Log out and log back in, then:
dd if=/dev/nbd0 of=./testdat_4k.dat bs=4k count=1

# Solution 3: Access via mounted filesystem (no sudo after mount)
sudo mkfs.ext4 /dev/nbd0
sudo mount /dev/nbd0 /mnt
echo "test" > /mnt/test.txt  # No sudo needed
cat /mnt/test.txt
```

### Testing Without Actual Hardware
If you don't have an OmniXtend device, you can still compile and test the initialization:
```bash
# This will fail at connection time, but shows proper initialization
sudo ./oxmem-bdev --netdev=lo --mac=00:00:00:00:00:00 --size=1G
```

## Performance Notes

- Read/write operations are chunked to 1024 bytes max
- Up to 20 concurrent requests in flight
- Each request is aligned to power-of-2 size
- NBD adds some overhead vs direct access, but provides standard interface

## Comparison with FUSE

| Aspect | oxmem-bdev | oxmem-fuse |
|--------|-----------|-----------|
| Code complexity | Simple | Complex |
| Lines of code | ~600 | 900+ |
| Filesystem support | Any | Single file only |
| Standard tools | Yes | No |
| Kernel module | NBD (built-in) | FUSE |
| Setup | modprobe nbd | Install libfuse-dev |
| mmap support | Yes (via filesystem) | Limited |

## License

Same as original oxmem-fuse code (see source files for details).

## See Also

- `CLAUDE.md` - Complete architecture documentation
- `README.md` - Original FUSE implementation docs
- `oxmem-fuse.c` - Original FUSE2 implementation
- `oxmem-fuse-dax.c` - FUSE3 with DAX support
