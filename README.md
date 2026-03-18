# zperf

A real-time ZFS performance dashboard for the terminal. Think `htop`, but for ZFS storage.

![Python](https://img.shields.io/badge/python-3.8+-blue) ![License](https://img.shields.io/badge/license-MIT-green) ![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20TrueNAS%20SCALE-lightgrey)

## Overview

zperf provides a single-screen view of your entire ZFS storage stack — from RAM cache down to spinning disks — with real-time performance metrics, efficiency comparisons between tiers, and intelligent status assessments.

```
── zperf ────────────────────────────────────────────────────────────────
 CPU ██░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 7.6%   MEM ████████████████████████████░░ 238G/251G   SWAP none
 ZFS  177 threads  CPU: 1.0%  [z_wr_iss:38 z_wr_int:32 z_fr_iss:20 z_rd_int:17 z_rd_iss:16]

── ARC (Memory Cache) [system-wide] ─────────────────────────────────────
  ████████░░ Hit: 81%    Hits: 294K  Misses: 70.3K  ~0.08ms  vs L2: 1.9x faster  vs Disk: 122x faster
  Size: 221G  Target: 221G  Max: 250G  Pressure: low
  ...

── L2ARC (NVMe Cache) [system-wide] ─────────────────────────────────────
  █████████░ Hit: 97%    Hits: 68.1K  Misses: 2.2K  vs Disk: 63x faster
  ...

── SLOG (Sync Write Log) ────────────────────────────────────────────────
  █████████░ Cap: 93%    Writes: 25/s  BW: 83K/s  Lat: 0.16ms  vs Disk: 13x faster
  ...

── Cache & Log Devices ──────────────────────────────────────────────────
  DEVICE                 R_OPS      W_OPS       R_BW       W_BW     R_LAT     W_LAT    BUSY  ACTIVITY
  nvme1n1p1 [SLOG]         0/s       13/s     0.0B/s      50K/s         -    0.14ms    0.0%  ░░░░░░░░░░

── Pool Data Devices ────────────────────────────────────────────────────
  sdc1                     51/s       28/s     1.2M/s     306K/s      10ms     1.0ms     42%  ████████░░
```

## Features

- **System overview**: CPU, memory, swap usage with visual bars, ZFS kernel thread summary
- **ARC (RAM cache)**: Hit rate, size/target/max, MRU/MFU split, demand vs prefetch hits, memory pressure indicator
- **L2ARC (NVMe read cache)**: Hit rate, feed status, read/write latency from device stats
- **SLOG (sync write log)**: Capture rate, ZIL commit/ITX stats, write amplification, TXG sync timing
- **Tier comparisons**: Each cache layer shows speedup vs the tier below it (e.g., "vs Disk: 63x faster")
- **Per-device I/O**: Read/write ops, bandwidth, latency, busy%, with latency outlier detection (>2x peers highlighted in red)
- **Smart status messages**: Context-aware assessments that distinguish demand misses from prefetch streaming
- **Interval/cumulative modes**: Toggle with `c` to see per-interval deltas or totals since startup
- **Interactive help**: Press `a`, `l`, `s`, or `d` for detailed field descriptions
- **Pool-aware**: Only shows L2ARC/SLOG sections for pools that have them configured
- **Zero dependencies**: Pure Python 3 stdlib (curses + /proc)

## Installation

Copy `zperf.py` to your ZFS server:

```bash
scp zperf.py user@server:/usr/local/bin/zperf
chmod +x /usr/local/bin/zperf
```

No pip install, no virtualenv, no dependencies. Just Python 3.8+ and a ZFS system.

## Usage

```bash
# Auto-detect pool, 2s refresh
sudo python3 zperf

# Specific pool
sudo python3 zperf jpool

# Custom refresh interval
sudo python3 zperf -n 5

# Start in cumulative mode
sudo python3 zperf -c

# All together
sudo python3 zperf jpool -n 3 -c
```

Requires root (or sudo) to read `/proc/spl/kstat/zfs/` and run `zpool` commands.

## Interactive Controls

| Key | Action |
|-----|--------|
| `1`-`9` | Set refresh interval (seconds) |
| `0` | Freeze display (pause updates) |
| `c` | Toggle interval / cumulative mode |
| `a` | Help: ARC field descriptions |
| `l` | Help: L2ARC field descriptions |
| `s` | Help: SLOG field descriptions |
| `d` | Help: Device table field descriptions |
| `q` / `ESC` | Quit |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ZFS_DASH_INTERVAL` | `2` | Refresh interval in seconds |
| `ZFS_DASH_POOL` | (auto) | Pool name |

## What the Sections Mean

### ARC (Adaptive Replacement Cache)
ZFS keeps frequently and recently accessed data in RAM. The ARC is the first place ZFS looks when reading data — a hit here avoids all disk I/O. The hit rate bar and "vs Disk" speedup show how effectively RAM is absorbing your workload.

### L2ARC (Level 2 ARC)
When data is evicted from ARC (RAM), ZFS can cache it on fast NVMe drives. L2ARC is a read-only cache — it helps when your working set is larger than RAM. The "vs Disk" speedup shows NVMe latency vs spinning disk latency.

### SLOG (Separate Log)
Synchronous writes (fsync, NFS, databases) must be on stable storage before ZFS returns. Without SLOG, these go to the pool drives. With SLOG, they go to fast NVMe, dramatically reducing sync write latency. The capture rate shows what percentage of sync writes are being accelerated.

### Device Tables
Per-device I/O from `/proc/diskstats`. Latency values shown in red indicate a device performing >2x slower than its peers — potential straggler drive.

## Compatibility

- **Linux** with OpenZFS (ZFS on Linux)
- **TrueNAS SCALE** (Debian-based, run with `python3 /path/to/zperf`)
- **Python 3.8+** (uses only stdlib)
- Requires `/proc/spl/kstat/zfs/` (OpenZFS kernel module)

Note: ARC and L2ARC statistics are system-wide (shared across all pools). Per-pool filtering is applied to SLOG, device tables, and pool capacity metrics.

## Screenshots

Text captures from a 7-disk raidz2 pool with mirrored NVMe SLOG and dual NVMe L2ARC on TrueNAS SCALE (256GB RAM):

- [Idle](screenshots/idle.txt) — system at rest, ARC 100% hit rate, SLOG capturing 93%
- [Under load](screenshots/under-load.txt) — sequential scan workload, L2ARC 97% hit rate at 63x faster than disk, pool drives at 42-48% busy

## Credits

Created by Chris Caho with [Claude Code](https://claude.ai/claude-code) (Anthropic Claude Opus 4.6).

## License

MIT
