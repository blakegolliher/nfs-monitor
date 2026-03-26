# nfs-monitor

A command-line tool for monitoring NFS mount performance in real-time on Linux. It reads kernel-level statistics from `/proc/self/mountstats`, samples at configurable intervals, and produces a detailed report covering latency, throughput, retransmissions, timeouts, and errors per NFS operation.

Includes a built-in `compare` subcommand for side-by-side comparison of two monitoring runs.

## Usage

### Monitor NFS mounts

```bash
# Monitor a specific mount for 60 seconds (default)
nfs-monitor --mp=/mnt/nfs1

# Monitor for 5 minutes with 5-second sample intervals
nfs-monitor --mp=/mnt/nfs1 -d 300 -i 5

# Monitor multiple mounts
nfs-monitor --mp=/mnt/nfs1 --mp=/mnt/nfs2 -d 300

# Monitor all NFS mounts
nfs-monitor -a -d 60

# You can also specify by device name
nfs-monitor --mp=server:/export -d 120

# List available NFS mounts and exit
nfs-monitor --list
```

### Compare two runs

Capture output from two monitoring sessions, then compare:

```bash
nfs-monitor -a -d 180 > baseline.out
# ... change something ...
nfs-monitor -a -d 180 > test.out

nfs-monitor compare baseline.out test.out
nfs-monitor compare baseline.out test.out Baseline Test
```

The comparison report shows ops/sec, latency, and per-operation breakdowns with ratios indicating which run performed better.

## Flags

| Flag | Long form | Default | Description |
|------|-----------|---------|-------------|
| `--mp` | | *(required unless `-a`)* | NFS mountpoint or device to monitor. Repeatable. |
| `-a` | `--all` | `false` | Monitor all NFS mounts. |
| `-d` | `--duration` | `60` | Monitoring duration in seconds. |
| `-i` | `--interval` | `1` | Sample interval in seconds. |
| | `--list` | `false` | List available NFS mounts and exit. |

### Compare subcommand

```
nfs-monitor compare <file1> <file2> [label1] [label2]
```

- `file1`, `file2` -- paths to captured nfs-monitor output files
- `label1`, `label2` -- optional display labels (default: "File1", "File2")

## Output

The monitoring report includes:

- **Summary** -- total operations, ops/sec, retransmissions, timeouts, errors
- **Latency table** -- per-operation ops count, ops/sec, RTT average/min/max
- **Breakdown tables** -- retransmissions, timeouts, and errors by operation (shown only when non-zero)

The tool also detects and warns about anomalies during sampling: counter resets, mountpoint changes, and device unmounts.

## Build

Requires Go 1.21 or later.

```bash
go build -o nfs-monitor .
```

To cross-compile a static Linux binary from macOS:

```bash
GOOS=linux GOARCH=amd64 go build -o nfs-monitor .
```
