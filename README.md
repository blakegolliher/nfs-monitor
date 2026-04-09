# nfs-monitor

> **Deprecated — see [nfs-gaze](https://github.com/blakegolliher/nfs-gaze).**
>
> nfs-monitor is no longer actively developed. Every feature in this
> tool (live monitoring, operation filtering, JSON snapshots via
> `-o`/`--output`, fixed-duration capture via `-d`/`--duration`, and
> the `compare` subcommand) has been folded into **nfs-gaze**, which
> is the successor. The JSON snapshot schema is preserved byte for
> byte, so baseline reports written with `nfs-monitor -o` remain
> readable by `nfs-gaze compare`.
>
> New captures, new features, and bug fixes should all happen in
> nfs-gaze. This repository is retained for historical reference.

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

# Save report to a file (JSON, for later comparison)
nfs-monitor -a -d 180 -o baseline.json

# List available NFS mounts and exit
nfs-monitor --list
```

### Compare two runs

Capture output from two monitoring sessions using `-o`, then compare:

```bash
nfs-monitor -a -d 180 -o baseline.json
# ... change something ...
nfs-monitor -a -d 180 -o test.json

nfs-monitor compare baseline.json test.json
nfs-monitor compare baseline.json test.json Baseline Test
```

The `-o` flag writes a **JSON** report to a file for the compare subcommand to consume. The human-readable text report still prints to stdout, and progress/warnings go to stderr. If you want the text report saved too, pipe stdout through `tee`:

```bash
nfs-monitor -a -d 180 -o baseline.json | tee baseline.txt
```

The comparison report shows ops/sec, latency, and per-operation breakdowns with ratios indicating which run performed better.

Example JSON files are included in `examples/` so you can try the compare feature without live NFS mounts:

```bash
nfs-monitor compare examples/baseline.json examples/test.json Baseline Test
```

## Flags

| Flag | Long form | Default | Description |
|------|-----------|---------|-------------|
| `--mp` | | *(required unless `-a`)* | NFS mountpoint or device to monitor. Repeatable. |
| `-a` | `--all` | `false` | Monitor all NFS mounts. |
| `-d` | `--duration` | `60` | Monitoring duration in seconds. |
| `-i` | `--interval` | `1` | Sample interval in seconds. |
| `-o` | `--output` | | Write JSON report to file (for later use with compare). |
| | `--list` | `false` | List available NFS mounts and exit. |

### Compare subcommand

```
nfs-monitor compare <file1> <file2> [label1] [label2]
```

- `file1`, `file2` -- paths to captured nfs-monitor JSON reports (written via `-o`)
- `label1`, `label2` -- optional display labels (default: "File1", "File2")

## Output

The monitoring report includes:

- **Summary** -- total operations, ops/sec, retransmissions, timeouts, errors
- **Latency table** -- per-operation ops count, ops/sec, RTT average/min/max
- **Breakdown tables** -- retransmissions, timeouts, and errors by operation (shown only when non-zero)

The tool also detects and warns about anomalies during sampling: counter resets, mountpoint changes, and device unmounts.

## Build & Install

Requires Go 1.21 or later.

```bash
make build           # produces ./nfs-monitor in the working directory
sudo make install    # installs to /usr/local/bin/nfs-monitor
```

Install to a different prefix:

```bash
sudo make install PREFIX=/opt/local
```

For packagers, `DESTDIR` is honored:

```bash
make install DESTDIR=/tmp/stage PREFIX=/usr
# stages the binary at /tmp/stage/usr/bin/nfs-monitor
```

Remove an installed copy:

```bash
sudo make uninstall              # removes /usr/local/bin/nfs-monitor
sudo make uninstall PREFIX=/opt/local
```

Other targets: `make test`, `make vet`, `make fmt`, `make clean`.

To cross-compile a static Linux binary from macOS:

```bash
GOOS=linux GOARCH=amd64 make build
```
