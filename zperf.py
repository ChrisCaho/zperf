#!/usr/bin/python3
"""zperf - ZFS performance dashboard.

Displays real-time stats for ZFS storage in an htop-like layout:
  - ARC (RAM cache) hit rates, pressure, and efficiency vs lower tiers
  - L2ARC (NVMe read cache) hit rates and speedup vs spinning disk
  - SLOG (sync write log) throughput, latency, and speedup vs disk
  - Per-device I/O: ops, bandwidth, latency, busy%, with outlier detection

Usage:
  sudo zperf [pool] [-n SECONDS] [-c]
"""

import argparse
import curses
import time
import os
import re
import subprocess
from pathlib import Path

REFRESH_INTERVAL = 2.0
try:
    REFRESH_INTERVAL = float(os.environ.get("ZFS_DASH_INTERVAL", "2"))
except ValueError:
    pass

POOL_NAME = os.environ.get("ZFS_DASH_POOL", "")

SECTOR_BYTES = 512  # Linux kernel sector size (not ZFS ashift)

# Estimated ARC lookup latency including ZFS overhead (hash, decompress, buffer mgmt).
# Raw DRAM is ~100ns, but ZFS adds overhead bringing it to ~50-100us range.
ARC_EST_LAT_MS = 0.08

ARC_COUNTER_KEYS = [
    "hits", "misses",
    "demand_data_hits", "demand_data_misses",
    "demand_metadata_hits", "demand_metadata_misses",
    "prefetch_data_hits", "prefetch_data_misses",
    "prefetch_metadata_hits", "prefetch_metadata_misses",
    "evict_l2_eligible", "evict_l2_ineligible",
    "l2_hits", "l2_misses", "l2_read_bytes", "l2_write_bytes",
    "l2_io_error", "l2_cksum_bad",
    "l2_feeds",
]

ZIL_COUNTER_KEYS = [
    "zil_commit_count", "zil_commit_writer_count",
    "zil_itx_count", "zil_itx_indirect_count", "zil_itx_indirect_bytes",
    "zil_itx_copied_count", "zil_itx_copied_bytes",
    "zil_itx_needcopy_count", "zil_itx_needcopy_bytes",
    "zil_itx_metaslab_slog_count", "zil_itx_metaslab_slog_bytes",
    "zil_itx_metaslab_normal_count", "zil_itx_metaslab_normal_bytes",
    "zil_itx_metaslab_slog_write",
    "zil_itx_metaslab_normal_write",
]


def read_file(path):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return ""


def build_uuid_to_dev_map():
    """Map partition UUIDs from /dev/disk/by-partuuid/ to device names."""
    mapping = {}
    partuuid_dir = Path("/dev/disk/by-partuuid")
    if not partuuid_dir.exists():
        return mapping
    for entry in partuuid_dir.iterdir():
        try:
            mapping[entry.name] = entry.resolve().name
        except OSError:
            pass
    return mapping


def parse_kstat(path):
    """Parse a /proc/spl/kstat file. Type 3=uint32, type 4=uint64."""
    stats = {}
    raw = read_file(path)
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[1] in ("3", "4"):
            try:
                stats[parts[0]] = int(parts[2])
            except ValueError:
                pass
    return stats


def get_cpu_times():
    raw = read_file("/proc/stat")
    for line in raw.splitlines():
        if line.startswith("cpu "):
            vals = [int(x) for x in line.split()[1:]]
            total = sum(vals)
            idle = vals[3] + (vals[4] if len(vals) > 4 else 0)  # idle + iowait
            return total, idle
    return 0, 0


def get_mem_info():
    info = {}
    raw = read_file("/proc/meminfo")
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) >= 2:
            try:
                info[parts[0].rstrip(":")] = int(parts[1]) * 1024  # kB -> bytes
            except ValueError:
                pass
    total = info.get("MemTotal", 0)
    used = total - info.get("MemAvailable", 0)
    swap_total = info.get("SwapTotal", 0)
    swap_free = info.get("SwapFree", 0)
    swap_used = swap_total - swap_free
    return total, used, swap_total, swap_used


def get_cpu_count():
    """Get number of CPU cores from /proc/stat (count 'cpu<N>' lines)."""
    raw = read_file("/proc/stat")
    cores = sum(1 for line in raw.splitlines() if re.match(r"^cpu\d", line))
    return cores or 1


# Cache CPU count at module level — never changes at runtime
_CPU_COUNT = None


def get_zfs_process_summary():
    """Summarize ZFS kernel threads into one ps-like line.
    Returns: (count, system_cpu_pct, thread_type_counts_str).
    CPU% is normalized to system total (divided by core count)."""
    global _CPU_COUNT
    if _CPU_COUNT is None:
        _CPU_COUNT = get_cpu_count()
    raw = run_cmd(["ps", "-eo", "comm,%cpu", "--no-headers"])
    total_cpu = 0.0
    count = 0
    types = {}
    for line in raw.splitlines():
        parts = line.strip().rsplit(None, 1)
        if len(parts) != 2:
            continue
        comm, cpu_str = parts
        # Match ZFS/SPL kernel threads: z_*, spl_*, zfs*, raidz_*
        if not (comm.startswith("z_") or comm.startswith("spl_") or
                comm.startswith("zfs") or comm.startswith("raidz")):
            continue
        try:
            cpu = float(cpu_str)
        except ValueError:
            continue
        count += 1
        total_cpu += cpu
        # Group by base name (strip trailing _N digits)
        base = re.sub(r"_?\d+$", "", comm)
        types[base] = types.get(base, 0) + 1
    # Normalize: ps reports per-core %, divide by cores for system %
    system_cpu = total_cpu / _CPU_COUNT
    sorted_types = sorted(types.items(), key=lambda x: -x[1])
    top_types = [f"{name}:{n}" for name, n in sorted_types[:6]]
    return count, system_cpu, " ".join(top_types)


def get_diskstats():
    """Parse /proc/diskstats. Fields: major minor name reads rd_merged rd_sectors rd_ms
    writes wr_merged wr_sectors wr_ms ios_in_progress io_ms weighted_io_ms ..."""
    stats = {}
    raw = read_file("/proc/diskstats")
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) < 14:
            continue
        try:
            stats[parts[2]] = {
                "reads": int(parts[3]),       # completed reads
                "read_ms": int(parts[6]),      # total ms spent on reads
                "writes": int(parts[7]),       # completed writes
                "write_ms": int(parts[10]),    # total ms spent on writes
                "read_sectors": int(parts[5]),
                "write_sectors": int(parts[9]),
                "io_ms": int(parts[12]),       # ms spent doing I/O (wall clock)
            }
        except (ValueError, IndexError):
            pass
    return stats


def parse_txg_stats(pool):
    """Parse recent TXG commit stats. Uses last 10 completed TXGs for averaging."""
    raw = read_file(f"/proc/spl/kstat/zfs/{pool}/txgs")
    lines = raw.strip().splitlines()
    # Columns: txg birth state ndirty nread nwritten reads writes otime qtime wtime stime
    # wtime=write time (ns), stime=sync time (ns)
    recent_stimes = []
    recent_wtimes = []
    for line in lines[-10:]:
        parts = line.split()
        if len(parts) >= 12 and parts[2] == "C":  # C = committed
            try:
                recent_stimes.append(int(parts[11]))  # stime (ns)
                recent_wtimes.append(int(parts[10]))   # wtime (ns)
            except (ValueError, IndexError):
                pass
    avg_sync_ms = (sum(recent_stimes) / len(recent_stimes) / 1_000_000) if recent_stimes else 0  # ns -> ms
    avg_write_ms = (sum(recent_wtimes) / len(recent_wtimes) / 1_000_000) if recent_wtimes else 0
    return avg_sync_ms, avg_write_ms


def run_cmd(cmd):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        return r.stdout
    except (OSError, subprocess.TimeoutExpired):
        return ""


def parse_zpool_status(pool):
    raw = run_cmd(["zpool", "status", pool])
    result = {"data": [], "log": [], "cache": [], "spares": []}
    section = "data"
    in_config = False
    for line in raw.splitlines():
        if "config:" in line:
            in_config = True
            continue
        if not in_config:
            continue
        if line.strip().startswith("errors:"):
            break
        stripped = line.strip()
        if stripped == "logs":
            section = "log"
            continue
        elif stripped == "cache":
            section = "cache"
            continue
        elif stripped == "spares":
            section = "spares"
            continue
        if stripped and not stripped.startswith("NAME") and not stripped.startswith("-"):
            parts = stripped.split()
            if parts:
                result[section].append(parts[0])
    return result


def parse_zpool_list(pool):
    """Get pool capacity: size, alloc, free, fragmentation, capacity%."""
    raw = run_cmd(["zpool", "list", "-Hp", "-o", "size,alloc,free,frag,cap", pool])
    parts = raw.strip().split()
    if len(parts) >= 5:
        try:
            return {
                "size": int(parts[0]),
                "alloc": int(parts[1]),
                "free": int(parts[2]),
                "frag": int(parts[3]),
                "cap": int(parts[4]),
            }
        except ValueError:
            pass
    return {}


def detect_pool():
    raw = run_cmd(["zpool", "list", "-H", "-o", "name"])
    for line in raw.splitlines():
        name = line.strip()
        if name and "boot" not in name.lower():
            return name
    for line in raw.splitlines():
        name = line.strip()
        if name:
            return name
    return ""


# ── Formatters ──────────────────────────────────────────────────────

def fmt_bytes(n):
    if n < 0:
        return "-" + fmt_bytes(-n)
    for unit in ("B", "K", "M", "G", "T", "P"):
        if abs(n) < 1024:
            return f"{n:.1f}{unit}" if n < 10 else f"{n:.0f}{unit}"
        n /= 1024
    return f"{n:.0f}E"


def fmt_rate(n):
    return fmt_bytes(n) + "/s"


def fmt_count(n):
    if n < 0:
        return "-" + fmt_count(-n)
    if n < 1000:
        return str(int(n))
    if n < 100_000:
        return f"{n/1000:.1f}K"
    if n < 1_000_000:
        return f"{n/1000:.0f}K"
    if n < 100_000_000:
        return f"{n/1_000_000:.1f}M"
    return f"{n/1_000_000:.0f}M"


def fmt_ops(n):
    if n < 1:
        return "0"
    if n < 10:
        return f"{n:.1f}"
    if n < 1000:
        return f"{n:.0f}"
    if n < 100_000:
        return f"{n/1000:.1f}K"
    return f"{n/1000:.0f}K"


def fmt_pct(n):
    return f"{n:.1f}%" if n < 10 else f"{n:.0f}%"


def fmt_lat(ms):
    if ms <= 0:
        return "-"
    if ms < 0.01:
        return f"{ms*1000:.0f}us"
    if ms < 1:
        return f"{ms:.2f}ms"
    if ms < 10:
        return f"{ms:.1f}ms"
    if ms < 1000:
        return f"{ms:.0f}ms"
    return f"{ms/1000:.1f}s"


def fmt_speedup(factor):
    if factor <= 0:
        return "-"
    if factor < 10:
        return f"{factor:.1f}x"
    return f"{factor:.0f}x"


def bar_str(pct, width):
    filled = max(0, min(width, int(pct / 100 * width)))
    return "\u2588" * filled + "\u2591" * (width - filled)


# ── Delta helpers ───────────────────────────────────────────────────

def compute_stat_deltas(cur, prev, keys):
    deltas = {}
    for k in keys:
        deltas[k] = cur.get(k, 0) - prev.get(k, 0)
    return deltas


# ── Dashboard ───────────────────────────────────────────────────────

class Dashboard:
    def __init__(self, stdscr, pool, cumulative=False):
        self.stdscr = stdscr
        self.pool = pool
        self.cumulative = cumulative
        self.refresh_interval = REFRESH_INTERVAL
        self.frozen = False
        self.prev_cpu = (0, 0)
        self.prev_disk = {}
        self.prev_time = time.monotonic()
        self._start_time = self.prev_time
        self.cpu_pct = 0.0
        self.uuid_map = build_uuid_to_dev_map()

        self.prev_arc = {}
        self.first_arc = {}
        self.prev_zil = {}
        self.first_zil = {}
        self.first_disk = {}

        # Status tracking: once a section reaches a good state, don't regress to "warming"
        self.l2arc_ever_hit = False
        self.arc_ever_active = False
        self.slog_ever_active = False

        # Pre-computed cross-section latency averages
        self.data_avg_r_lat = 0
        self.data_avg_w_lat = 0
        self.slog_avg_w_lat = 0
        self.l2arc_avg_r_lat = 0

        # Cached subprocess results with TTLs to reduce fork overhead
        self._vdev_info = {}
        self._vdev_info_time = 0
        self._pool_info = {}
        self._pool_info_time = 0
        self._zfs_procs = (0, 0.0, "")
        self._zfs_procs_time = 0

    def _get_vdev_info(self):
        """Return cached vdev topology, refreshing every 30s."""
        now = time.monotonic()
        if not self._vdev_info or (now - self._vdev_info_time) > 30:
            self._vdev_info = parse_zpool_status(self.pool)
            self._vdev_info_time = now
        return self._vdev_info

    def _get_pool_info(self):
        """Return cached pool capacity info, refreshing every 15s."""
        now = time.monotonic()
        if not self._pool_info or (now - self._pool_info_time) > 15:
            self._pool_info = parse_zpool_list(self.pool)
            self._pool_info_time = now
        return self._pool_info

    def _get_zfs_procs(self):
        """Return cached ZFS thread summary, refreshing every 5s."""
        now = time.monotonic()
        if (now - self._zfs_procs_time) > 5:
            self._zfs_procs = get_zfs_process_summary()
            self._zfs_procs_time = now
        return self._zfs_procs

    def update_cpu(self):
        total, idle = get_cpu_times()
        pt, pi = self.prev_cpu
        dt = total - pt
        di = idle - pi
        if dt > 0:
            self.cpu_pct = 100.0 * (1.0 - di / dt)
        self.prev_cpu = (total, idle)

    def update_disk_deltas(self):
        now = time.monotonic()
        elapsed = now - self.prev_time
        if elapsed < 0.1:  # clamp to avoid jitter from sub-100ms intervals
            elapsed = 1.0
        self.prev_time = now

        cur = get_diskstats()

        if not self.first_disk:
            self.first_disk = {k: dict(v) for k, v in cur.items()}

        ref = self.first_disk if self.cumulative else self.prev_disk
        if not ref:
            ref = cur

        deltas = {}
        for dev, s in cur.items():
            p = ref.get(dev, s)
            d_reads = s["reads"] - p["reads"]
            d_writes = s["writes"] - p["writes"]
            d_read_ms = s["read_ms"] - p["read_ms"]
            d_write_ms = s["write_ms"] - p["write_ms"]
            # Average latency per I/O operation over the interval
            r_lat = (d_read_ms / d_reads) if d_reads > 0 else 0
            w_lat = (d_write_ms / d_writes) if d_writes > 0 else 0
            d_elapsed = elapsed if not self.cumulative else (now - self._start_time)
            if d_elapsed < 0.1:
                d_elapsed = 1.0
            deltas[dev] = {
                "r_ops": d_reads / d_elapsed,
                "w_ops": d_writes / d_elapsed,
                "r_bytes": (s["read_sectors"] - p["read_sectors"]) * SECTOR_BYTES / d_elapsed,
                "w_bytes": (s["write_sectors"] - p["write_sectors"]) * SECTOR_BYTES / d_elapsed,
                "io_ms": s["io_ms"] - p["io_ms"],
                "elapsed": d_elapsed,
                "r_lat_ms": r_lat,
                "w_lat_ms": w_lat,
            }
        self.prev_disk = cur
        return deltas

    def update_arc_stats(self):
        cur = parse_kstat("/proc/spl/kstat/zfs/arcstats")
        if not self.first_arc:
            self.first_arc = dict(cur)
        ref = self.first_arc if self.cumulative else self.prev_arc
        if not ref:
            ref = cur
        deltas = compute_stat_deltas(cur, ref, ARC_COUNTER_KEYS)
        self.prev_arc = cur
        return cur, deltas

    def update_zil_stats(self):
        cur = parse_kstat("/proc/spl/kstat/zfs/zil")
        if not self.first_zil:
            self.first_zil = dict(cur)
        ref = self.first_zil if self.cumulative else self.prev_zil
        if not ref:
            ref = cur
        deltas = compute_stat_deltas(cur, ref, ZIL_COUNTER_KEYS)
        self.prev_zil = cur
        return cur, deltas

    def _compute_device_group_latency(self, disk_deltas, vdev_info, group):
        """Average read/write latency for a vdev group (data, log, or cache)."""
        r_lats = []
        w_lats = []
        for dev in vdev_info.get(group, []):
            if dev == self.pool or dev.startswith("mirror") or dev.startswith("raidz") or dev.startswith("spare"):
                continue
            _, d = self._resolve_dev(dev, disk_deltas)
            if not d:
                continue
            if d.get("r_lat_ms", 0) > 0:
                r_lats.append(d["r_lat_ms"])
            if d.get("w_lat_ms", 0) > 0:
                w_lats.append(d["w_lat_ms"])
        avg_r = sum(r_lats) / len(r_lats) if r_lats else 0
        avg_w = sum(w_lats) / len(w_lats) if w_lats else 0
        return avg_r, avg_w

    def safe_addstr(self, row, col, text, *args):
        h, w = self.stdscr.getmaxyx()
        if row >= h or row < 0:
            return
        avail = w - col - 1
        if avail <= 0:
            return
        try:
            self.stdscr.addstr(row, col, text[:avail], *args)
        except curses.error:
            pass

    def draw_header(self, row, mem_total, mem_used, swap_total, swap_used, zfs_procs):
        h, w = self.stdscr.getmaxyx()

        # Title bar
        title_line = f"\u2500\u2500 zperf " + "\u2500" * max(0, w - 9)
        self.safe_addstr(row, 0, title_line[:w-1], curses.color_pair(4) | curses.A_BOLD)
        row += 1

        # Reserve ~50 chars for labels, divide remaining among bars
        bar_w = max(10, min(30, (w - 60) // 3))

        cpu_bar = bar_str(self.cpu_pct, bar_w)
        mem_pct = (mem_used / mem_total * 100) if mem_total else 0
        mem_bar = bar_str(mem_pct, bar_w)

        self.safe_addstr(row, 1, "CPU ", curses.color_pair(1) | curses.A_BOLD)
        self.safe_addstr(row, 5, cpu_bar, curses.color_pair(2))
        cpu_end = 5 + bar_w + 1
        self.safe_addstr(row, cpu_end, fmt_pct(self.cpu_pct), curses.color_pair(1))

        mem_start = cpu_end + 7
        self.safe_addstr(row, mem_start, "MEM ", curses.color_pair(1) | curses.A_BOLD)
        self.safe_addstr(row, mem_start + 4, mem_bar, curses.color_pair(3))
        mem_end = mem_start + 4 + bar_w + 1
        self.safe_addstr(row, mem_end, f"{fmt_bytes(mem_used)}/{fmt_bytes(mem_total)}", curses.color_pair(1))

        swap_start = mem_end + 16
        if swap_total > 0:
            swap_pct = (swap_used / swap_total * 100)
            swap_bar_w = max(5, min(15, bar_w // 2))
            swap_bar = bar_str(swap_pct, swap_bar_w)
            self.safe_addstr(row, swap_start, "SWAP ", curses.color_pair(1) | curses.A_BOLD)
            # Red >50%, yellow >10%, green otherwise
            swap_bar_color = curses.color_pair(6) if swap_pct > 50 else curses.color_pair(7) if swap_pct > 10 else curses.color_pair(5)
            self.safe_addstr(row, swap_start + 5, swap_bar, swap_bar_color)
            swap_text_start = swap_start + 5 + swap_bar_w + 1
            self.safe_addstr(row, swap_text_start, f"{fmt_bytes(swap_used)}/{fmt_bytes(swap_total)}", curses.color_pair(1))
        else:
            self.safe_addstr(row, swap_start, "SWAP none", curses.color_pair(1) | curses.A_BOLD)
        row += 1

        # ZFS process summary line
        zfs_count, zfs_cpu, zfs_types = zfs_procs
        if zfs_count > 0:
            proc_line = f"ZFS  {zfs_count} threads  CPU: {zfs_cpu:.1f}%  [{zfs_types}]"
        else:
            proc_line = "ZFS  no kernel threads found"
        self.safe_addstr(row, 1, proc_line, curses.color_pair(1))
        return row + 1

    def draw_section_header(self, row, title):
        h, w = self.stdscr.getmaxyx()
        line = f"\u2500\u2500 {title} " + "\u2500" * max(0, w - len(title) - 5)
        self.safe_addstr(row, 0, line[:w-1], curses.color_pair(4) | curses.A_BOLD)
        return row + 1

    def draw_arc(self, row, arc_raw, arc_delta, pool_info):
        hits = arc_delta.get("hits", 0)
        misses = arc_delta.get("misses", 0)
        total_acc = hits + misses
        hit_pct = (hits / total_acc * 100) if total_acc else 0

        size = arc_raw.get("size", 0)
        target = arc_raw.get("c", 0)
        c_max = arc_raw.get("c_max", 0)
        mru = arc_raw.get("mru_size", 0)
        mfu = arc_raw.get("mfu_size", 0)

        demand_hits = arc_delta.get("demand_data_hits", 0) + arc_delta.get("demand_metadata_hits", 0)
        demand_misses = arc_delta.get("demand_data_misses", 0) + arc_delta.get("demand_metadata_misses", 0)
        prefetch_hits = arc_delta.get("prefetch_data_hits", 0) + arc_delta.get("prefetch_metadata_hits", 0)
        prefetch_misses = arc_delta.get("prefetch_data_misses", 0) + arc_delta.get("prefetch_metadata_misses", 0)
        evict_total = arc_delta.get("evict_l2_eligible", 0) + arc_delta.get("evict_l2_ineligible", 0)

        # ARC pressure: how far target has been pushed below max by memory pressure.
        # <5% = healthy, 30-60% = moderate contention, >60% = significant memory pressure.
        pressure_pct = ((c_max - target) / c_max * 100) if c_max > 0 else 0
        if pressure_pct < 5:
            pressure_str = "none"
            pressure_color = curses.color_pair(5)
        elif pressure_pct < 30:
            pressure_str = "low"
            pressure_color = curses.color_pair(5)
        elif pressure_pct < 60:
            pressure_str = "moderate"
            pressure_color = curses.color_pair(7)
        else:
            pressure_str = "HIGH"
            pressure_color = curses.color_pair(6)

        # Efficiency: ARC (RAM) vs L2ARC (NVMe) vs data drives (HDD/SSD)
        speedup_vs_l2 = (self.l2arc_avg_r_lat / ARC_EST_LAT_MS) if self.l2arc_avg_r_lat > 0 else 0
        speedup_vs_disk = (self.data_avg_r_lat / ARC_EST_LAT_MS) if self.data_avg_r_lat > 0 else 0

        spd_l2 = fmt_speedup(speedup_vs_l2) if speedup_vs_l2 > 0 else "N/A"
        spd_disk = fmt_speedup(speedup_vs_disk) if speedup_vs_disk > 0 else "N/A"

        row = self.draw_section_header(row, "ARC (Memory Cache) [system-wide]")
        # Healthy ARC typically >90%; flag below 85%
        color = curses.color_pair(5) if hit_pct >= 85 else curses.color_pair(6)
        arc_bar = bar_str(hit_pct, 10)
        self.safe_addstr(row, 2, arc_bar, color)
        self.safe_addstr(row, 13, "Hit: ", curses.color_pair(1))
        self.safe_addstr(row, 18, fmt_pct(hit_pct), color | curses.A_BOLD)
        col = 25
        base = f"Hits: {fmt_count(hits)}  Misses: {fmt_count(misses)}  ~{fmt_lat(ARC_EST_LAT_MS)}  vs L2: "
        self.safe_addstr(row, col, base, curses.color_pair(1))
        col += len(base)
        if speedup_vs_l2 > 1:
            self.safe_addstr(row, col, spd_l2 + " faster", curses.color_pair(5) | curses.A_BOLD)
            col += len(spd_l2 + " faster")
        else:
            self.safe_addstr(row, col, spd_l2, curses.color_pair(1))
            col += len(spd_l2)
        self.safe_addstr(row, col, "  vs Disk: ", curses.color_pair(1))
        col += len("  vs Disk: ")
        if speedup_vs_disk > 1:
            self.safe_addstr(row, col, spd_disk + " faster", curses.color_pair(5) | curses.A_BOLD)
        else:
            self.safe_addstr(row, col, spd_disk, curses.color_pair(1))
        row += 1

        pressure_line = f"Size: {fmt_bytes(size)}  Target: {fmt_bytes(target)}  Max: {fmt_bytes(c_max)}  Pressure: "
        self.safe_addstr(row, 2, pressure_line, curses.color_pair(1))
        self.safe_addstr(row, 2 + len(pressure_line), pressure_str, pressure_color | curses.A_BOLD)
        row += 1

        self.safe_addstr(row, 2, f"MRU: {fmt_bytes(mru)}  MFU: {fmt_bytes(mfu)}  Evictions: {fmt_count(evict_total)}", curses.color_pair(1))
        row += 1

        d_total = demand_hits + demand_misses
        d_pct = (demand_hits / d_total * 100) if d_total else 0
        p_total = prefetch_hits + prefetch_misses
        p_pct = (prefetch_hits / p_total * 100) if p_total else 0
        frag = pool_info.get("frag", 0)
        cap = pool_info.get("cap", 0)
        self.safe_addstr(row, 2, f"Demand Hit: {fmt_pct(d_pct)}  Prefetch Hit: {fmt_pct(p_pct)}  Pool Used: {cap}%  Frag: {frag}%", curses.color_pair(1))
        row += 1

        # Status line — factor in demand vs prefetch for smarter assessment
        if total_acc > 0:
            self.arc_ever_active = True
        if hit_pct >= 95:
            self.safe_addstr(row, 2, "ARC excellent - nearly all reads served from memory", curses.color_pair(5))
        elif hit_pct >= 85:
            self.safe_addstr(row, 2, "ARC healthy - good hit rate, working set fits well", curses.color_pair(5))
        elif d_pct >= 95 and hit_pct >= 50:
            # Demand reads are fine, overall hit rate is low due to prefetch/sequential misses
            self.safe_addstr(row, 2, "ARC demand reads healthy - prefetch/sequential streaming through cache", curses.color_pair(5))
        elif hit_pct >= 50:
            self.safe_addstr(row, 2, "ARC under pressure - working set exceeds cache, consider more RAM", curses.color_pair(7))
        elif d_pct >= 85 and total_acc > 0:
            self.safe_addstr(row, 2, "ARC demand OK but heavy sequential I/O bypassing cache", curses.color_pair(7))
        elif total_acc > 0:
            self.safe_addstr(row, 2, "ARC struggling - high miss rate, significant disk I/O", curses.color_pair(6))
        elif self.arc_ever_active:
            self.safe_addstr(row, 2, "ARC idle - no read activity this interval", curses.color_pair(7))
        else:
            self.safe_addstr(row, 2, "ARC starting up - waiting for read activity", curses.color_pair(7))
        return row + 2

    def draw_l2arc(self, row, arc_raw, arc_delta, disk_deltas, cache_devs):
        hits = arc_delta.get("l2_hits", 0)
        misses = arc_delta.get("l2_misses", 0)
        total_acc = hits + misses
        hit_pct = (hits / total_acc * 100) if total_acc else 0

        size = arc_raw.get("l2_asize", 0) or arc_raw.get("l2_size", 0)
        read_bytes = arc_delta.get("l2_read_bytes", 0)
        write_bytes = arc_delta.get("l2_write_bytes", 0)
        errs = arc_delta.get("l2_io_error", 0) + arc_delta.get("l2_cksum_bad", 0)
        feeds = arc_delta.get("l2_feeds", 0)  # ARC->L2ARC feed cycles this interval

        r_lats = []
        w_lats = []
        for dev in cache_devs:
            _, d = self._resolve_dev(dev, disk_deltas)
            if not d:
                continue
            if d.get("r_lat_ms", 0) > 0:
                r_lats.append(d["r_lat_ms"])
            if d.get("w_lat_ms", 0) > 0:
                w_lats.append(d["w_lat_ms"])
        avg_r_lat = sum(r_lats) / len(r_lats) if r_lats else 0
        avg_w_lat = sum(w_lats) / len(w_lats) if w_lats else 0

        # Efficiency: L2ARC vs data drives only (compare to lower tier)
        speedup = (self.data_avg_r_lat / avg_r_lat) if avg_r_lat > 0 and self.data_avg_r_lat > 0 else 0
        spd_disk = fmt_speedup(speedup) if speedup > 0 else "N/A"

        row = self.draw_section_header(row, "L2ARC (NVMe Cache) [system-wide]")

        color = curses.color_pair(5) if hit_pct >= 50 else curses.color_pair(7) if hit_pct > 0 else curses.color_pair(1)
        l2_bar = bar_str(hit_pct, 10)
        self.safe_addstr(row, 2, l2_bar, color)
        self.safe_addstr(row, 13, "Hit: ", curses.color_pair(1))
        self.safe_addstr(row, 18, fmt_pct(hit_pct), color | curses.A_BOLD)
        col = 25
        base = f"Hits: {fmt_count(hits)}  Misses: {fmt_count(misses)}  vs Disk: "
        self.safe_addstr(row, col, base, curses.color_pair(1))
        col += len(base)
        if speedup > 1:
            self.safe_addstr(row, col, spd_disk + " faster", curses.color_pair(5) | curses.A_BOLD)
        else:
            self.safe_addstr(row, col, spd_disk, curses.color_pair(1))
        row += 1

        self.safe_addstr(row, 2, f"Size: {fmt_bytes(size)}  Read: {fmt_bytes(read_bytes)}  Written: {fmt_bytes(write_bytes)}  Feeds: {fmt_count(feeds)}  Errors: {fmt_count(errs)}", curses.color_pair(1))
        row += 1

        self.safe_addstr(row, 2, f"Read Latency: {fmt_lat(avg_r_lat)}  Write Latency: {fmt_lat(avg_w_lat)}", curses.color_pair(1))
        row += 1

        if hits > 0:
            self.l2arc_ever_hit = True
            self.safe_addstr(row, 2, "L2ARC active - serving read hits", curses.color_pair(5))
        elif self.l2arc_ever_hit:
            self.safe_addstr(row, 2, "L2ARC idle - no read hits this interval", curses.color_pair(7))
        elif write_bytes > 0:
            self.safe_addstr(row, 2, "L2ARC warming up - feeding data, no read hits yet", curses.color_pair(7))
        else:
            self.safe_addstr(row, 2, "L2ARC idle - no activity this interval", curses.color_pair(7))
        return row + 2

    def draw_slog(self, row, disk_deltas, log_devs, txg_sync_ms, txg_write_ms, zil_delta):
        row = self.draw_section_header(row, "SLOG (Sync Write Log)")
        if not log_devs:
            self.safe_addstr(row, 2, "No SLOG configured", curses.color_pair(7))
            return row + 2

        total_w_ops = 0
        total_w_bytes = 0
        w_lats = []
        actual_devs = []
        for dev in log_devs:
            if dev.startswith("mirror") or dev.startswith("spare"):
                continue
            actual_devs.append(dev)
            _, d = self._resolve_dev(dev, disk_deltas)
            if not d:
                continue
            total_w_ops += d.get("w_ops", 0)
            total_w_bytes += d.get("w_bytes", 0)
            if d.get("w_lat_ms", 0) > 0:
                w_lats.append(d["w_lat_ms"])
        avg_w_lat = sum(w_lats) / len(w_lats) if w_lats else 0

        # ZIL intent transaction (ITX) stats
        commits = zil_delta.get("zil_commit_count", 0)
        itxs = zil_delta.get("zil_itx_count", 0)
        slog_count = zil_delta.get("zil_itx_metaslab_slog_count", 0)
        slog_bytes = zil_delta.get("zil_itx_metaslab_slog_bytes", 0)
        # "Bypass" = ZIL writes that went directly to pool (too large for SLOG)
        normal_count = zil_delta.get("zil_itx_metaslab_normal_count", 0)
        normal_bytes = zil_delta.get("zil_itx_metaslab_normal_bytes", 0)
        total_zil_ops = slog_count + normal_count

        # Write amplification: physical bytes written / logical bytes
        slog_phys = zil_delta.get("zil_itx_metaslab_slog_write", 0)
        write_amp = (slog_phys / slog_bytes) if slog_bytes > 0 else 0

        # SLOG capture rate: % of ZIL metaslab ops that went to SLOG vs direct to pool
        capture_pct = (slog_count / total_zil_ops * 100) if total_zil_ops > 0 else 0

        # Efficiency: SLOG write latency vs data drive write latency
        speedup = (self.data_avg_w_lat / avg_w_lat) if avg_w_lat > 0 and self.data_avg_w_lat > 0 else 0
        spd_disk = fmt_speedup(speedup) if speedup > 0 else "N/A"

        # Capture bar on first line
        capture_color = curses.color_pair(5) if capture_pct >= 80 else curses.color_pair(7) if capture_pct >= 50 else curses.color_pair(6)
        slog_bar = bar_str(capture_pct, 10)
        self.safe_addstr(row, 2, slog_bar, capture_color)
        self.safe_addstr(row, 13, "Cap: ", curses.color_pair(1))
        self.safe_addstr(row, 18, fmt_pct(capture_pct), capture_color | curses.A_BOLD)
        col = 25
        base = f"Writes: {fmt_ops(total_w_ops)}/s  BW: {fmt_rate(total_w_bytes)}  Lat: {fmt_lat(avg_w_lat)}  vs Disk: "
        self.safe_addstr(row, col, base, curses.color_pair(1))
        col += len(base)
        if speedup > 1:
            self.safe_addstr(row, col, spd_disk + " faster", curses.color_pair(5) | curses.A_BOLD)
        else:
            self.safe_addstr(row, col, spd_disk, curses.color_pair(1))
        row += 1

        self.safe_addstr(row, 2, f"ZIL Commits: {fmt_count(commits)}  ITXs: {fmt_count(itxs)}  SLOG: {fmt_count(slog_count)} ({fmt_bytes(slog_bytes)})  Bypass: {fmt_count(normal_count)} ({fmt_bytes(normal_bytes)})", curses.color_pair(1))
        row += 1

        wa_str = f"Write Amp: {write_amp:.1f}x  " if write_amp > 0 else ""
        # TXG sync time = how long periodic transaction group commits take
        self.safe_addstr(row, 2, f"{wa_str}TXG Sync: {fmt_lat(txg_sync_ms)}  TXG Write: {fmt_lat(txg_write_ms)}", curses.color_pair(1))
        row += 1

        # Status line — once active, only show idle or performance states
        if total_w_ops > 0:
            self.slog_ever_active = True
        if total_w_ops > 0 and capture_pct >= 80 and speedup > 1:
            self.safe_addstr(row, 2, f"SLOG effective - capturing {fmt_pct(capture_pct)} of sync writes at NVMe speed", curses.color_pair(5))
        elif total_w_ops > 0 and capture_pct < 50:
            self.safe_addstr(row, 2, "SLOG underutilized - most sync writes bypassing to pool (large writes?)", curses.color_pair(7))
        elif total_w_ops > 0:
            self.safe_addstr(row, 2, "SLOG active - handling sync writes", curses.color_pair(5))
        elif self.slog_ever_active:
            self.safe_addstr(row, 2, "SLOG idle - no sync write activity this interval", curses.color_pair(7))
        else:
            self.safe_addstr(row, 2, "SLOG waiting - no sync write activity yet", curses.color_pair(7))
        return row + 2

    # ── Device tables ───────────────────────────────────────────────

    COL_DEV = 22
    COL_OPS = 10
    COL_BW = 10
    COL_LAT = 9
    COL_BUSY = 7

    def _dev_table_header(self, row):
        hdr = (f"  {'DEVICE':<{self.COL_DEV}}"
               f" {'R_OPS':>{self.COL_OPS}} {'W_OPS':>{self.COL_OPS}}"
               f" {'R_BW':>{self.COL_BW}} {'W_BW':>{self.COL_BW}}"
               f" {'R_LAT':>{self.COL_LAT}} {'W_LAT':>{self.COL_LAT}}"
               f" {'BUSY':>{self.COL_BUSY}}  ACTIVITY")
        self.safe_addstr(row, 0, hdr, curses.color_pair(4))
        return row + 1

    def _dev_table_row(self, row, dev, d, tag, peer_r_lat=0, peer_w_lat=0):
        h, w = self.stdscr.getmaxyx()
        if row >= h - 2:
            return row

        r_ops = d.get("r_ops", 0)
        w_ops = d.get("w_ops", 0)
        r_bw = d.get("r_bytes", 0)
        w_bw = d.get("w_bytes", 0)
        r_lat = d.get("r_lat_ms", 0)
        w_lat = d.get("w_lat_ms", 0)
        elapsed = d.get("elapsed", 2)
        io_ms = d.get("io_ms", 0)
        # Busy%: io_ms (wall-clock ms in I/O) / elapsed_ms * 100
        # Formula: io_ms / (elapsed_seconds * 1000) * 100 = io_ms / elapsed / 10
        busy_pct = min(100, io_ms / (elapsed * 10)) if elapsed > 0 else 0

        fixed_cols = 2 + self.COL_DEV + 2*self.COL_OPS + 2*self.COL_BW + 2*self.COL_LAT + self.COL_BUSY + 12
        bar_w = max(5, min(20, w - fixed_cols))
        activity = bar_str(busy_pct, bar_w)

        dev_display = f"{dev} {tag}" if tag else dev
        line = (f"  {dev_display:<{self.COL_DEV}}"
                f" {fmt_ops(r_ops)+'/s':>{self.COL_OPS}} {fmt_ops(w_ops)+'/s':>{self.COL_OPS}}"
                f" {fmt_rate(r_bw):>{self.COL_BW}} {fmt_rate(w_bw):>{self.COL_BW}}")
        self.safe_addstr(row, 0, line, curses.color_pair(1))

        # Flag latency outliers: >2x the group average indicates a straggler drive
        r_lat_str = f" {fmt_lat(r_lat):>{self.COL_LAT}}"
        r_lat_color = curses.color_pair(1)
        if r_lat > 0 and peer_r_lat > 0 and r_lat > peer_r_lat * 2:
            r_lat_color = curses.color_pair(6) | curses.A_BOLD
        self.safe_addstr(row, len(line), r_lat_str, r_lat_color)

        w_lat_str = f" {fmt_lat(w_lat):>{self.COL_LAT}}"
        w_lat_color = curses.color_pair(1)
        if w_lat > 0 and peer_w_lat > 0 and w_lat > peer_w_lat * 2:
            w_lat_color = curses.color_pair(6) | curses.A_BOLD
        col_after_lat = len(line) + len(r_lat_str) + len(w_lat_str)
        self.safe_addstr(row, len(line) + len(r_lat_str), w_lat_str, w_lat_color)

        busy_str = f" {fmt_pct(busy_pct):>{self.COL_BUSY}}  "
        self.safe_addstr(row, col_after_lat, busy_str, curses.color_pair(1))

        if busy_pct > 80:
            color = curses.color_pair(6)
        elif busy_pct > 40:
            color = curses.color_pair(8)
        else:
            color = curses.color_pair(5)
        self.safe_addstr(row, col_after_lat + len(busy_str), activity, color)
        return row + 1

    def _draw_dev_table(self, row, title, devs_with_deltas):
        row = self.draw_section_header(row, title)
        row = self._dev_table_header(row)
        if not devs_with_deltas:
            self.safe_addstr(row, 2, "No devices found", curses.color_pair(7))
            return row + 2

        # Compute peer averages for outlier detection
        r_lats = [d.get("r_lat_ms", 0) for _, d, _ in devs_with_deltas if d.get("r_lat_ms", 0) > 0]
        w_lats = [d.get("w_lat_ms", 0) for _, d, _ in devs_with_deltas if d.get("w_lat_ms", 0) > 0]
        peer_r = sum(r_lats) / len(r_lats) if r_lats else 0
        peer_w = sum(w_lats) / len(w_lats) if w_lats else 0

        for dev, d, tag in devs_with_deltas:
            row = self._dev_table_row(row, dev, d, tag, peer_r, peer_w)
        return row + 1

    def _resolve_dev(self, dev, disk_deltas):
        """Resolve a device name to a diskstats entry.
        Tries: direct match -> UUID lookup -> strip partition suffix to base device."""
        if dev in disk_deltas:
            return dev, disk_deltas[dev]
        resolved = self.uuid_map.get(dev, dev)
        if resolved in disk_deltas:
            return resolved, disk_deltas[resolved]
        # Strip partition suffix: sdi1->sdi, nvme1n1p1->nvme1n1
        # NVMe: remove trailing pN. SCSI: remove trailing digits.
        if "nvme" in resolved:
            base = re.sub(r"p\d+$", "", resolved)
        else:
            base = re.sub(r"\d+$", "", resolved)
        if base and base in disk_deltas:
            return base, disk_deltas[base]
        return None, None

    @staticmethod
    def _nat_sort_key(item):
        """Natural sort key: split name into text/number chunks for proper ordering."""
        return [int(c) if c.isdigit() else c.lower() for c in re.split(r"(\d+)", item[0])]

    def draw_intermediary_devices(self, row, disk_deltas, vdev_info):
        slog_devs = []
        for dev in vdev_info.get("log", []):
            if dev.startswith("mirror") or dev.startswith("spare"):
                continue
            name, d = self._resolve_dev(dev, disk_deltas)
            if d:
                slog_devs.append((name, d, "[SLOG]"))
        l2arc_devs = []
        for dev in vdev_info.get("cache", []):
            name, d = self._resolve_dev(dev, disk_deltas)
            if d:
                l2arc_devs.append((name, d, "[L2ARC]"))
        # Sort each group alphanumerically, SLOG first then L2ARC
        slog_devs.sort(key=self._nat_sort_key)
        l2arc_devs.sort(key=self._nat_sort_key)
        return self._draw_dev_table(row, "Cache & Log Devices", slog_devs + l2arc_devs)

    def draw_data_devices(self, row, disk_deltas, vdev_info):
        devs = []
        for dev in vdev_info.get("data", []):
            if dev == self.pool or dev.startswith("mirror") or dev.startswith("raidz") or dev.startswith("spare"):
                continue
            name, d = self._resolve_dev(dev, disk_deltas)
            if d:
                devs.append((name, d, ""))
        devs.sort(key=self._nat_sort_key)
        return self._draw_dev_table(row, "Pool Data Devices", devs)

    def draw_footer(self, row):
        h, w = self.stdscr.getmaxyx()
        if row >= h - 1:
            row = h - 1
        mode = "CUMULATIVE" if self.cumulative else "INTERVAL"
        if self.frozen:
            rate = "FROZEN"
        else:
            r = self.refresh_interval
            rate = f"{r:.1f}s" if r != int(r) else f"{int(r)}s"
        footer = f" {self.pool}  {mode}  {rate}  |  0:freeze 1-9:interval  c:mode  a:ARC  l:L2  s:SLOG  d:Devs  q:quit "
        self.safe_addstr(row, 0, footer[:w-1], curses.color_pair(4) | curses.A_REVERSE)

    # ── Help overlays ───────────────────────────────────────────────

    HELP_ARC = [
        ("ARC (Adaptive Replacement Cache)", True),
        ("", False),
        ("ZFS keeps frequently and recently accessed data in RAM. The ARC is the", False),
        ("first place ZFS looks when reading data -- a hit here avoids all disk I/O.", False),
        ("", False),
        ("FIELDS:", True),
        ("Hit Rate     % of reads served from RAM. >90% is healthy, <50% is trouble.", False),
        ("Hits/Misses  Number of reads that found/missed data in ARC this interval.", False),
        ("~0.08ms      Estimated ARC lookup latency (RAM + ZFS overhead).", False),
        ("vs L2        How many times faster ARC is than L2ARC (NVMe cache).", False),
        ("vs Disk      How many times faster ARC is than reading from pool drives.", False),
        ("Size         Current ARC size in RAM.", False),
        ("Target       Size ZFS is aiming for (adapts based on memory pressure).", False),
        ("Max          Maximum ARC is allowed to grow.", False),
        ("Pressure     How much the OS has squeezed ARC below its max.", False),
        ("             none/low = healthy, moderate = contention, HIGH = starved.", False),
        ("MRU          Most Recently Used cache size (data accessed once recently).", False),
        ("MFU          Most Frequently Used cache size (data accessed repeatedly).", False),
        ("Evictions    Blocks evicted from ARC (eligible for L2ARC + ineligible).", False),
        ("Demand Hit   Hit rate for application-requested reads.", False),
        ("Prefetch Hit Hit rate for speculative read-ahead by ZFS.", False),
        ("Pool Used    % of pool storage capacity in use.", False),
        ("Frag         Pool fragmentation %. High frag can hurt write performance.", False),
        ("", False),
        ("STATUS LINE:", True),
        ("excellent    >95% hit rate, nearly everything from RAM.", False),
        ("healthy      >85% hit rate, working set fits well.", False),
        ("under pressure  50-85%, consider adding RAM.", False),
        ("struggling   <50%, heavy disk I/O, RAM badly undersized for workload.", False),
    ]

    HELP_L2ARC = [
        ("L2ARC (Level 2 Adaptive Replacement Cache)", True),
        ("", False),
        ("When data is evicted from ARC (RAM), ZFS can cache it on fast NVMe drives.", False),
        ("L2ARC is a read cache only -- it never handles writes. It helps when your", False),
        ("working set is larger than RAM but fits on NVMe.", False),
        ("", False),
        ("FIELDS:", True),
        ("Hit Rate     % of L2ARC lookups that found data. 0% is common early on.", False),
        ("Hits/Misses  L2ARC read hits and misses this interval.", False),
        ("vs Disk      How many times faster L2ARC reads are than pool data drives.", False),
        ("Size         Compressed size of data currently in L2ARC.", False),
        ("Read         Bytes read from L2ARC devices this interval.", False),
        ("Written      Bytes fed into L2ARC from ARC evictions.", False),
        ("Feeds        Number of ARC-to-L2ARC feed cycles (batches of evicted data).", False),
        ("Errors       I/O errors + checksum failures on L2ARC devices.", False),
        ("Read Latency Avg read latency of L2ARC NVMe devices (from diskstats).", False),
        ("Write Latency Avg write latency of L2ARC NVMe devices.", False),
        ("", False),
        ("STATUS LINE:", True),
        ("active       L2ARC is serving read hits -- it's working.", False),
        ("warming up   Data is being fed in but no reads have hit yet.", False),
        ("             This is normal after adding L2ARC or rebooting.", False),
        ("idle         No activity. If ARC hit rate is very high, L2ARC may", False),
        ("             not be needed -- everything already fits in RAM.", False),
        ("", False),
        ("NOTE: l2arc_noprefetch=1 (default) means sequential/prefetch reads", False),
        ("bypass L2ARC entirely. Only random demand reads benefit.", False),
    ]

    HELP_SLOG = [
        ("SLOG (Separate Log / Sync Write Log)", True),
        ("", False),
        ("When an application requests a synchronous write (fsync, O_SYNC, NFS),", False),
        ("ZFS must guarantee the data is on stable storage before returning.", False),
        ("Without SLOG, this write goes to the pool drives. With SLOG, it goes", False),
        ("to a fast NVMe device, dramatically reducing sync write latency.", False),
        ("", False),
        ("FIELDS:", True),
        ("Sync Writes  Write operations per second on SLOG devices.", False),
        ("Bandwidth    Bytes/s written to SLOG devices.", False),
        ("Latency      Average write latency of SLOG NVMe devices.", False),
        ("vs Disk      How many times faster SLOG writes are than pool drive writes.", False),
        ("ZIL Commits  Number of ZIL (ZFS Intent Log) commit operations.", False),
        ("ITXs         Intent transactions -- individual write records in the ZIL.", False),
        ("SLOG         ITXs that went to the SLOG device (fast path).", False),
        ("Bypass       ITXs too large for SLOG, written directly to pool (slow path).", False),
        ("SLOG Capture % of ZIL metaslab writes that went to SLOG vs pool.", False),
        ("             >80% is good. Low capture means large writes bypassing SLOG.", False),
        ("Write Amp    Ratio of physical to logical bytes on SLOG (overhead).", False),
        ("TXG Sync     Avg time to commit a transaction group (periodic flush).", False),
        ("TXG Write    Avg write phase time within a TXG commit.", False),
        ("", False),
        ("STATUS LINE:", True),
        ("effective    High capture rate + measurable speedup vs disk.", False),
        ("active       Handling sync writes.", False),
        ("underutilized  Most writes bypassing SLOG (too large or wrong workload).", False),
        ("idle         No sync write activity (e.g., no NFS, databases, or VMs).", False),
    ]

    HELP_DEVS = [
        ("Device Tables: Cache & Log Devices / Pool Data Devices", True),
        ("", False),
        ("These tables show per-device I/O statistics from /proc/diskstats.", False),
        ("Cache & Log shows your NVMe SLOG and L2ARC partitions.", False),
        ("Pool Data shows the actual drives in your raidz/mirror vdev.", False),
        ("", False),
        ("COLUMNS:", True),
        ("DEVICE       Device name with role tag: [SLOG] or [L2ARC].", False),
        ("R_OPS        Read operations per second.", False),
        ("W_OPS        Write operations per second.", False),
        ("R_BW         Read bandwidth (bytes/sec).", False),
        ("W_BW         Write bandwidth (bytes/sec).", False),
        ("R_LAT        Average read latency per I/O (ms spent / ops completed).", False),
        ("             Shown in RED if >2x the group average (straggler drive).", False),
        ("W_LAT        Average write latency per I/O.", False),
        ("             Shown in RED if >2x the group average.", False),
        ("BUSY         % of wall-clock time the device spent doing I/O.", False),
        ("             Can exceed expectations on parallel I/O devices.", False),
        ("ACTIVITY     Visual bar of busy%. Green <40%, Yellow 40-80%, Red >80%.", False),
        ("", False),
        ("LATENCY OUTLIERS:", True),
        ("If one drive is significantly slower than its peers (>2x avg latency),", False),
        ("its latency value turns RED+BOLD. This can indicate:", False),
        ("  - A failing drive     - Drive in different speed class", False),
        ("  - Thermal throttling  - Rebuilding/resilvering", False),
    ]

    def show_help(self, content):
        """Display a help overlay and wait for any key to dismiss."""
        self.stdscr.erase()
        h, w = self.stdscr.getmaxyx()
        for i, (line, is_header) in enumerate(content):
            if i >= h - 2:
                break
            attr = curses.color_pair(4) | curses.A_BOLD if is_header else curses.color_pair(1)
            self.safe_addstr(i, 2, line, attr)
        self.safe_addstr(h - 1, 0, " Press any key to return to dashboard ", curses.color_pair(4) | curses.A_REVERSE)
        try:
            self.stdscr.refresh()
        except curses.error:
            pass
        self.stdscr.nodelay(False)
        self.stdscr.getch()
        self.stdscr.nodelay(True)
        self.stdscr.timeout(int(self.refresh_interval * 1000))

    def run(self):
        try:
            curses.curs_set(0)
        except curses.error:
            pass  # some terminals don't support invisible cursor

        self.stdscr.nodelay(True)
        self.stdscr.timeout(int(self.refresh_interval * 1000))

        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_WHITE, -1)     # default text
        curses.init_pair(2, curses.COLOR_CYAN, -1)      # CPU bar
        curses.init_pair(3, curses.COLOR_MAGENTA, -1)    # MEM bar
        curses.init_pair(4, curses.COLOR_BLUE, -1)       # headers/dividers
        curses.init_pair(5, curses.COLOR_GREEN, -1)      # good / positive
        curses.init_pair(6, curses.COLOR_RED, -1)        # bad / outlier
        curses.init_pair(7, curses.COLOR_YELLOW, -1)     # warning / N/A
        curses.init_pair(8, curses.COLOR_WHITE, -1)      # medium busy (neutral)

        # Seed initial reads to establish baseline for delta computation
        self.prev_cpu = get_cpu_times()
        self.prev_disk = get_diskstats()
        self.first_disk = {k: dict(v) for k, v in self.prev_disk.items()}
        self.prev_arc = parse_kstat("/proc/spl/kstat/zfs/arcstats")
        self.first_arc = dict(self.prev_arc)
        self.prev_zil = parse_kstat("/proc/spl/kstat/zfs/zil")
        self.first_zil = dict(self.prev_zil)
        self.prev_time = time.monotonic()
        self._start_time = self.prev_time
        # Brief pause so first interval has meaningful deltas
        time.sleep(0.5)

        while True:
            self.stdscr.erase()

            # Gather data
            self.update_cpu()
            mem_total, mem_used, swap_total, swap_used = get_mem_info()
            arc_raw, arc_delta = self.update_arc_stats()
            zil_raw, zil_delta = self.update_zil_stats()
            disk_deltas = self.update_disk_deltas()
            vdev_info = self._get_vdev_info()
            txg_sync_ms, txg_write_ms = parse_txg_stats(self.pool)
            pool_info = self._get_pool_info()
            zfs_procs = self._get_zfs_procs()

            # Pre-compute all device group latencies before drawing
            self.data_avg_r_lat, self.data_avg_w_lat = self._compute_device_group_latency(
                disk_deltas, vdev_info, "data")
            self.l2arc_avg_r_lat, _ = self._compute_device_group_latency(
                disk_deltas, vdev_info, "cache")
            _, self.slog_avg_w_lat = self._compute_device_group_latency(
                disk_deltas, vdev_info, "log")

            # Determine which sections this pool actually has
            has_cache = any(d for d in vdev_info.get("cache", []) if not d.startswith("mirror") and not d.startswith("spare"))
            has_log = any(d for d in vdev_info.get("log", []) if not d.startswith("mirror") and not d.startswith("spare"))

            # Draw
            row = 0
            row = self.draw_header(row, mem_total, mem_used, swap_total, swap_used, zfs_procs)
            row += 1
            row = self.draw_arc(row, arc_raw, arc_delta, pool_info)
            if has_cache:
                row = self.draw_l2arc(row, arc_raw, arc_delta, disk_deltas, vdev_info.get("cache", []))
            if has_log:
                row = self.draw_slog(row, disk_deltas, vdev_info.get("log", []), txg_sync_ms, txg_write_ms, zil_delta)
            if has_cache or has_log:
                row = self.draw_intermediary_devices(row, disk_deltas, vdev_info)
            row = self.draw_data_devices(row, disk_deltas, vdev_info)
            self.draw_footer(self.stdscr.getmaxyx()[0] - 1)

            try:
                self.stdscr.refresh()
            except curses.error:
                pass  # terminal resize race

            # If frozen, block until a key is pressed
            if self.frozen:
                self.stdscr.nodelay(False)
                ch = self.stdscr.getch()
                self.stdscr.nodelay(True)
                self.stdscr.timeout(int(self.refresh_interval * 1000))
            else:
                ch = self.stdscr.getch()

            if ch in (ord("q"), ord("Q"), 27):
                break
            elif ch in (ord("c"), ord("C")):
                self.cumulative = not self.cumulative
            elif ch in (ord("a"), ord("A")):
                self.show_help(self.HELP_ARC)
            elif ch in (ord("l"), ord("L")):
                self.show_help(self.HELP_L2ARC)
            elif ch in (ord("s"), ord("S")):
                self.show_help(self.HELP_SLOG)
            elif ch in (ord("d"), ord("D")):
                self.show_help(self.HELP_DEVS)
            elif ch == ord("0"):
                self.frozen = True
            elif ord("1") <= ch <= ord("9"):
                self.frozen = False
                self.refresh_interval = ch - ord("0")
                self.stdscr.timeout(int(self.refresh_interval * 1000))


def main():
    parser = argparse.ArgumentParser(description="zperf - htop-style TUI for ZFS pool monitoring")
    parser.add_argument("pool", nargs="?", default=None, help="ZFS pool name (auto-detects if omitted)")
    parser.add_argument("-n", "--interval", type=float, default=None, help="Refresh interval in seconds (default: 2)")
    parser.add_argument("-c", "--cumulative", action="store_true", help="Start in cumulative mode (default: interval)")
    args = parser.parse_args()

    global REFRESH_INTERVAL
    if args.interval is not None:
        REFRESH_INTERVAL = args.interval
    # Validate pool exists
    available = [p.strip() for p in run_cmd(["zpool", "list", "-H", "-o", "name"]).splitlines() if p.strip()]
    if args.pool:
        if args.pool not in available:
            print(f"Error: Pool '{args.pool}' not found.")
            if available:
                print(f"Available pools: {', '.join(available)}")
            else:
                print("No ZFS pools found on this system.")
            return 1
        pool = args.pool
    else:
        pool = POOL_NAME or detect_pool()
    if not pool:
        print("Error: No ZFS pool found. Specify a pool name or set ZFS_DASH_POOL.")
        if available:
            print(f"Available pools: {', '.join(available)}")
        return 1

    mode = "cumulative" if args.cumulative else "interval"
    print(f"Starting zperf for pool: {pool} (refresh: {REFRESH_INTERVAL}s, mode: {mode})")
    try:
        curses.wrapper(lambda stdscr: Dashboard(stdscr, pool, cumulative=args.cumulative).run())
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    exit(main())
