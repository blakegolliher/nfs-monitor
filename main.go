package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Mountstats types
// ---------------------------------------------------------------------------

// OpStats holds per-operation statistics from mountstats.
type OpStats struct {
	Ops       int64
	Trans     int64
	Timeouts  int64
	BytesSent int64
	BytesRecv int64
	QueueMs   int64
	RttMs     int64
	ExecMs    int64
	Errors    int64
}

// MountInfo holds information about an NFS mount.
type MountInfo struct {
	Device     string
	MountPoint string
	FSType     string
	Options    string
	Ops        map[string]*OpStats
}

// OpDelta holds the delta between two samples for an operation.
type OpDelta struct {
	Ops      int64
	Retrans  int64
	Timeouts int64
	Errors   int64
	RttMs    int64
	ExecMs   int64
}

// AggregatedOp holds aggregated statistics for an operation across samples.
type AggregatedOp struct {
	OpsTotal       int64
	RetransTotal   int64
	TimeoutsTotal  int64
	ErrorsTotal    int64
	RttWeightedMs  int64
	ExecWeightedMs int64
	RttMinMs       float64
	RttMaxMs       float64
	seenRtt        bool
}

// ---------------------------------------------------------------------------
// Report types (serialized to JSON for -o and consumed by compare)
// ---------------------------------------------------------------------------

const reportSchemaVersion = 1

// Report is the top-level structured output of a monitoring run.
type Report struct {
	SchemaVersion int           `json:"schema_version"`
	GeneratedAt   time.Time     `json:"generated_at"`
	DurationSec   int           `json:"duration_sec"`
	IntervalSec   int           `json:"interval_sec"`
	Samples       int           `json:"samples"`
	Mounts        []MountReport `json:"mounts"`
}

// MountReport is the per-mount section of a Report.
type MountReport struct {
	Device     string       `json:"device"`
	MountPoint string       `json:"mountpoint"`
	FSType     string       `json:"fstype"`
	Options    string       `json:"options"`
	Summary    SummaryStats `json:"summary"`
	Operations []OpReport   `json:"operations"`
}

// SummaryStats holds mount-level totals.
type SummaryStats struct {
	TotalOps  int64   `json:"total_ops"`
	OpsPerSec float64 `json:"ops_per_sec"`
	Retrans   int64   `json:"retransmissions"`
	Timeouts  int64   `json:"timeouts"`
	Errors    int64   `json:"errors"`
}

// OpReport holds per-operation aggregated stats, ordered by Ops descending.
type OpReport struct {
	Name      string  `json:"name"`
	Ops       int64   `json:"ops"`
	OpsPerSec float64 `json:"ops_per_sec"`
	Retrans   int64   `json:"retransmissions"`
	Timeouts  int64   `json:"timeouts"`
	Errors    int64   `json:"errors"`
	RttAvgMs  float64 `json:"rtt_avg_ms"`
	RttMinMs  float64 `json:"rtt_min_ms"`
	RttMaxMs  float64 `json:"rtt_max_ms"`
}

// ---------------------------------------------------------------------------
// mountstats parsing
// ---------------------------------------------------------------------------

var (
	deviceRe = regexp.MustCompile(`^device\s+(\S+)\s+mounted on\s+(\S+)\s+with fstype\s+(\S+)`)
	opRe     = regexp.MustCompile(`^\s*(\w+):\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*(\d+)?`)
)

// stringSlice is a flag type that collects repeated values.
type stringSlice []string

func (s *stringSlice) String() string     { return strings.Join(*s, ", ") }
func (s *stringSlice) Set(v string) error { *s = append(*s, v); return nil }

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	if len(os.Args) > 1 && os.Args[1] == "compare" {
		runCompare(os.Args[2:])
		return
	}

	var (
		mountpoints stringSlice
		allMounts   bool
		duration    int
		interval    int
		listMounts  bool
		outputFile  string
	)

	flag.Var(&mountpoints, "mp", "NFS mountpoint to monitor (can specify multiple)")
	flag.BoolVar(&allMounts, "a", false, "Monitor all NFS mounts")
	flag.BoolVar(&allMounts, "all", false, "Monitor all NFS mounts")
	flag.IntVar(&duration, "d", 60, "Monitoring duration in seconds")
	flag.IntVar(&duration, "duration", 60, "Monitoring duration in seconds")
	flag.IntVar(&interval, "i", 1, "Sample interval in seconds")
	flag.IntVar(&interval, "interval", 1, "Sample interval in seconds")
	flag.BoolVar(&listMounts, "list", false, "List available NFS mounts and exit")
	flag.StringVar(&outputFile, "o", "", "Write JSON report to file (for later use with compare)")
	flag.StringVar(&outputFile, "output", "", "Write JSON report to file (for later use with compare)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "NFS Mount Statistics Monitor\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  %s --mp=/mnt/nfs1 -d 300\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s --mp=/mnt/nfs1 --mp=/mnt/nfs2 -d 300 -i 5\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -a -d 60\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s --list\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s compare <file1> <file2> [label1] [label2]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		fmt.Fprintf(os.Stderr, "  --mp=MOUNTPOINT       NFS mountpoint to monitor (can specify multiple)\n")
		fmt.Fprintf(os.Stderr, "  -a, --all             Monitor all NFS mounts\n")
		fmt.Fprintf(os.Stderr, "  -d, --duration=SECS   Monitoring duration in seconds (default 60)\n")
		fmt.Fprintf(os.Stderr, "  -i, --interval=SECS   Sample interval in seconds (default 1)\n")
		fmt.Fprintf(os.Stderr, "  -o, --output=FILE     Write JSON report to file (for later use with compare)\n")
		fmt.Fprintf(os.Stderr, "  --list                List available NFS mounts and exit\n")
		fmt.Fprintf(os.Stderr, "\nSubcommands:\n")
		fmt.Fprintf(os.Stderr, "  compare               Compare two nfs-monitor JSON reports\n")
	}

	flag.Parse()

	if len(mountpoints) > 0 && allMounts {
		fmt.Fprintln(os.Stderr, "Error: Cannot specify both --mp and -a/--all. Choose one.")
		os.Exit(1)
	}

	if !listMounts && len(mountpoints) == 0 && !allMounts {
		fmt.Fprintln(os.Stderr, "Error: Must specify either --mp=MOUNTPOINT or -a/--all")
		flag.Usage()
		os.Exit(1)
	}

	if interval > duration {
		fmt.Fprintf(os.Stderr, "Error: interval (%ds) must be <= duration (%ds)\n", interval, duration)
		os.Exit(1)
	}

	allMountStats, err := readMountstats("/proc/self/mountstats")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading mountstats: %v\n", err)
		os.Exit(1)
	}

	if len(allMountStats) == 0 {
		fmt.Println("No NFS mounts found.")
		os.Exit(0)
	}

	if listMounts {
		fmt.Println("Available NFS mounts (device and mountpoint):")
		for _, device := range sortedKeys(allMountStats) {
			fmt.Printf("  %s (on %s)\n", device, allMountStats[device].MountPoint)
		}
		os.Exit(0)
	}

	var mounts map[string]*MountInfo
	if allMounts {
		mounts = allMountStats
	} else {
		mounts = make(map[string]*MountInfo)
		var missing []string
		for _, target := range mountpoints {
			if info, ok := allMountStats[target]; ok {
				mounts[target] = info
				continue
			}
			found := false
			for device, info := range allMountStats {
				if info.MountPoint == target {
					mounts[device] = info
					found = true
					break
				}
			}
			if !found {
				missing = append(missing, target)
			}
		}

		if len(missing) > 0 {
			fmt.Fprintf(os.Stderr, "Error: Device(s) or Mountpoint(s) not found: %s\n", strings.Join(missing, ", "))
			fmt.Fprintln(os.Stderr, "\nAvailable NFS devices and their mountpoints:")
			for _, device := range sortedKeys(allMountStats) {
				fmt.Fprintf(os.Stderr, "  %s (on %s)\n", device, allMountStats[device].MountPoint)
			}
			os.Exit(1)
		}
	}

	if len(mounts) == 0 {
		fmt.Fprintln(os.Stderr, "Error: No matching NFS mounts found for monitoring.")
		os.Exit(1)
	}

	// Validate we can create the output file before spending the sample window on monitoring.
	var outFile *os.File
	if outputFile != "" {
		f, err := os.Create(outputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		outFile = f
	}

	printHeader(os.Stdout)
	printMountInfo(os.Stdout, mounts)

	fmt.Fprintf(os.Stderr, "\nStarting %ds monitoring (interval: %ds)...\n", duration, interval)
	fmt.Fprintln(os.Stderr, "Run your workload now.")
	fmt.Fprintln(os.Stderr, strings.Repeat("-", 40))

	samples := collectSamples(duration, interval, mounts)
	if len(samples) == 0 {
		fmt.Fprintln(os.Stderr, "No samples collected.")
		os.Exit(1)
	}

	aggregated := aggregateSamples(samples)
	report := buildReport(aggregated, mounts, duration, interval, len(samples))

	writeTextReport(os.Stdout, report)

	if outFile != nil {
		if err := writeJSONReport(outFile, report); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing report: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "JSON report written to %s\n", outputFile)
	}
}

// ---------------------------------------------------------------------------
// Mountstats parsing
// ---------------------------------------------------------------------------

func readMountstats(path string) (map[string]*MountInfo, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	mounts := make(map[string]*MountInfo)
	var current *MountInfo
	var inPerOp bool

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "device ") {
			if matches := deviceRe.FindStringSubmatch(line); matches != nil {
				fstype := matches[3]
				if strings.Contains(fstype, "nfs") {
					current = &MountInfo{
						Device:     matches[1],
						MountPoint: matches[2],
						FSType:     fstype,
						Ops:        make(map[string]*OpStats),
					}
					mounts[current.Device] = current
					inPerOp = false
				} else {
					current = nil
				}
			}
			continue
		}

		if current == nil {
			continue
		}

		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "opts:") {
			current.Options = strings.TrimSpace(strings.TrimPrefix(trimmed, "opts:"))
			continue
		}

		if strings.Contains(strings.ToLower(line), "per-op statistics") {
			inPerOp = true
			continue
		}

		if inPerOp && strings.Contains(line, ":") {
			if opName, stats := parsePerOpLine(line); stats != nil {
				current.Ops[opName] = stats
			}
		}
	}

	return mounts, scanner.Err()
}

func parsePerOpLine(line string) (string, *OpStats) {
	matches := opRe.FindStringSubmatch(line)
	if matches == nil {
		return "", nil
	}
	stats := &OpStats{
		Ops:       parseInt(matches[2]),
		Trans:     parseInt(matches[3]),
		Timeouts:  parseInt(matches[4]),
		BytesSent: parseInt(matches[5]),
		BytesRecv: parseInt(matches[6]),
		QueueMs:   parseInt(matches[7]),
		RttMs:     parseInt(matches[8]),
		ExecMs:    parseInt(matches[9]),
	}
	if matches[10] != "" {
		stats.Errors = parseInt(matches[10])
	}
	return matches[1], stats
}

// ---------------------------------------------------------------------------
// Sampling
// ---------------------------------------------------------------------------

func collectSamples(durationSec, intervalSec int, targetMounts map[string]*MountInfo) []map[string]map[string]*OpDelta {
	var samples []map[string]map[string]*OpDelta

	targetDevices := make(map[string]string)
	for device, info := range targetMounts {
		targetDevices[device] = info.MountPoint
	}

	prev := make(map[string]*MountInfo)
	for device, info := range targetMounts {
		prev[device] = copyMountInfo(info)
	}

	startTime := time.Now()
	sampleNum := 0
	resetCount := 0

	for time.Since(startTime).Seconds() < float64(durationSec) {
		time.Sleep(time.Duration(intervalSec) * time.Second)

		allCurr, err := readMountstats("/proc/self/mountstats")
		if err != nil {
			continue
		}

		curr := make(map[string]*MountInfo)
		for device, info := range allCurr {
			if _, ok := targetMounts[device]; !ok {
				continue
			}
			if targetDevices[device] != info.MountPoint {
				fmt.Fprintf(os.Stderr, "\nWarning: Mountpoint for device %s changed from %s to %s\n",
					device, targetDevices[device], info.MountPoint)
				targetDevices[device] = info.MountPoint
				delete(prev, device)
			}
			curr[device] = info
		}

		sampleDeltas := make(map[string]map[string]*OpDelta)

		for device, prevInfo := range prev {
			mountpoint := targetDevices[device]
			if mountpoint == "" {
				mountpoint = "unknown"
			}
			currInfo, ok := curr[device]
			if !ok {
				fmt.Fprintf(os.Stderr, "\nWarning: Device %s (at %s) is no longer mounted.\n", device, mountpoint)
				delete(prev, device)
				continue
			}

			if sampleDeltas[device] == nil {
				sampleDeltas[device] = make(map[string]*OpDelta)
			}

			for opName, prevOp := range prevInfo.Ops {
				currOp, ok := currInfo.Ops[opName]
				if !ok {
					continue
				}

				delta, isReset := computeDelta(prevOp, currOp)
				if isReset {
					resetCount++
					fmt.Fprintf(os.Stderr, "\nWarning: Counter reset detected for op '%s' on mount '%s'. Skipping sample.\n", opName, mountpoint)
					delete(prev, device)
					break
				}

				if delta.Ops > 0 || delta.Retrans > 0 || delta.Timeouts > 0 || delta.Errors > 0 {
					sampleDeltas[device][opName] = delta
				}
			}
		}

		if len(sampleDeltas) > 0 {
			samples = append(samples, sampleDeltas)
		}

		for device, info := range curr {
			if _, ok := prev[device]; !ok {
				targetDevices[device] = info.MountPoint
			}
			prev[device] = copyMountInfo(info)
		}

		sampleNum++
		elapsed := int(time.Since(startTime).Seconds())
		fmt.Fprintf(os.Stderr, "\r  Sampling... %3ds / %ds  (%d samples)", elapsed, durationSec, sampleNum)
	}

	fmt.Fprintln(os.Stderr)

	if resetCount > 0 {
		fmt.Fprintf(os.Stderr, "  Note: %d counter resets detected (likely due to remount or stats reset).\n", resetCount)
	}

	return samples
}

func computeDelta(prev, curr *OpStats) (*OpDelta, bool) {
	dOps := curr.Ops - prev.Ops
	dTrans := curr.Trans - prev.Trans
	dTimeouts := curr.Timeouts - prev.Timeouts
	dErrors := curr.Errors - prev.Errors
	dRtt := curr.RttMs - prev.RttMs
	dExec := curr.ExecMs - prev.ExecMs

	if dOps < 0 || dTrans < 0 {
		return nil, true
	}

	dRetrans := dTrans - dOps
	if dRetrans < 0 {
		dRetrans = 0
	}

	return &OpDelta{
		Ops:      dOps,
		Retrans:  dRetrans,
		Timeouts: max(0, dTimeouts),
		Errors:   max(0, dErrors),
		RttMs:    max(0, dRtt),
		ExecMs:   max(0, dExec),
	}, false
}

func copyMountInfo(info *MountInfo) *MountInfo {
	cp := &MountInfo{
		Device:     info.Device,
		MountPoint: info.MountPoint,
		FSType:     info.FSType,
		Options:    info.Options,
		Ops:        make(map[string]*OpStats),
	}
	for name, op := range info.Ops {
		cp.Ops[name] = &OpStats{
			Ops:       op.Ops,
			Trans:     op.Trans,
			Timeouts:  op.Timeouts,
			BytesSent: op.BytesSent,
			BytesRecv: op.BytesRecv,
			QueueMs:   op.QueueMs,
			RttMs:     op.RttMs,
			ExecMs:    op.ExecMs,
			Errors:    op.Errors,
		}
	}
	return cp
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

func aggregateSamples(samples []map[string]map[string]*OpDelta) map[string]map[string]*AggregatedOp {
	agg := make(map[string]map[string]*AggregatedOp)

	for _, sampleDeltas := range samples {
		for device, opsDeltas := range sampleDeltas {
			if agg[device] == nil {
				agg[device] = make(map[string]*AggregatedOp)
			}

			for opName, delta := range opsDeltas {
				a := agg[device][opName]
				if a == nil {
					a = &AggregatedOp{
						RttMinMs: math.Inf(1),
						RttMaxMs: math.Inf(-1),
					}
					agg[device][opName] = a
				}

				a.OpsTotal += delta.Ops
				a.RetransTotal += delta.Retrans
				a.TimeoutsTotal += delta.Timeouts
				a.ErrorsTotal += delta.Errors
				a.RttWeightedMs += delta.RttMs
				a.ExecWeightedMs += delta.ExecMs

				if delta.Ops > 0 {
					avgRtt := float64(delta.RttMs) / float64(delta.Ops)
					if avgRtt < a.RttMinMs {
						a.RttMinMs = avgRtt
					}
					if avgRtt > a.RttMaxMs {
						a.RttMaxMs = avgRtt
					}
					a.seenRtt = true
				}
			}
		}
	}

	return agg
}

// ---------------------------------------------------------------------------
// Report construction (the canonical data shape; text + JSON both consume this)
// ---------------------------------------------------------------------------

func buildReport(agg map[string]map[string]*AggregatedOp, mounts map[string]*MountInfo, durationSec, intervalSec, numSamples int) *Report {
	report := &Report{
		SchemaVersion: reportSchemaVersion,
		GeneratedAt:   time.Now().UTC(),
		DurationSec:   durationSec,
		IntervalSec:   intervalSec,
		Samples:       numSamples,
	}

	// Union of devices from requested mounts and sample-visible devices,
	// so unmounted-mid-run devices and silent mounts both appear.
	seen := make(map[string]bool)
	var devices []string
	for device := range mounts {
		if !seen[device] {
			devices = append(devices, device)
			seen[device] = true
		}
	}
	for device := range agg {
		if !seen[device] {
			devices = append(devices, device)
			seen[device] = true
		}
	}
	sort.Strings(devices)

	for _, device := range devices {
		mr := MountReport{Device: device}
		if info := mounts[device]; info != nil {
			mr.MountPoint = info.MountPoint
			mr.FSType = info.FSType
			mr.Options = info.Options
		}

		opsData := agg[device]
		var totalOps, totalRetrans, totalTimeouts, totalErrors int64
		for _, op := range opsData {
			totalOps += op.OpsTotal
			totalRetrans += op.RetransTotal
			totalTimeouts += op.TimeoutsTotal
			totalErrors += op.ErrorsTotal
		}

		opsPerSec := 0.0
		if durationSec > 0 {
			opsPerSec = float64(totalOps) / float64(durationSec)
		}
		mr.Summary = SummaryStats{
			TotalOps:  totalOps,
			OpsPerSec: opsPerSec,
			Retrans:   totalRetrans,
			Timeouts:  totalTimeouts,
			Errors:    totalErrors,
		}

		for name, op := range opsData {
			if op.OpsTotal == 0 {
				continue
			}
			avgRtt := float64(op.RttWeightedMs) / float64(op.OpsTotal)
			minRtt, maxRtt := op.RttMinMs, op.RttMaxMs
			if !op.seenRtt {
				minRtt, maxRtt = 0, 0
			}
			perOpSec := 0.0
			if durationSec > 0 {
				perOpSec = float64(op.OpsTotal) / float64(durationSec)
			}
			mr.Operations = append(mr.Operations, OpReport{
				Name:      name,
				Ops:       op.OpsTotal,
				OpsPerSec: perOpSec,
				Retrans:   op.RetransTotal,
				Timeouts:  op.TimeoutsTotal,
				Errors:    op.ErrorsTotal,
				RttAvgMs:  avgRtt,
				RttMinMs:  minRtt,
				RttMaxMs:  maxRtt,
			})
		}
		sort.Slice(mr.Operations, func(i, j int) bool {
			return mr.Operations[i].Ops > mr.Operations[j].Ops
		})

		report.Mounts = append(report.Mounts, mr)
	}

	return report
}

// ---------------------------------------------------------------------------
// Report output: text and JSON
// ---------------------------------------------------------------------------

func writeJSONReport(w io.Writer, report *Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

func loadReport(path string) (*Report, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r Report
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &r, nil
}

func printHeader(w io.Writer) {
	fmt.Fprintln(w, strings.Repeat("=", 75))
	fmt.Fprintln(w, "NFS Mount Statistics Monitor")
	fmt.Fprintln(w, strings.Repeat("=", 75))
}

func printMountInfo(w io.Writer, mounts map[string]*MountInfo) {
	fmt.Fprintln(w, "\nNFS Mounts to Monitor:")
	fmt.Fprintln(w, strings.Repeat("-", 70))

	for _, device := range sortedKeys(mounts) {
		info := mounts[device]
		fmt.Fprintf(w, "\n  %s (on %s)\n", device, info.MountPoint)
		fmt.Fprintf(w, "    Type:   %s\n", info.FSType)

		if info.Options != "" {
			fmt.Fprintln(w, "    Options:")
			opts := strings.Split(info.Options, ",")
			line := "      "
			isSoft := false
			for _, opt := range opts {
				opt = strings.TrimSpace(opt)
				if opt == "soft" {
					isSoft = true
				}
				if len(line)+len(opt)+2 > 75 {
					fmt.Fprintln(w, line)
					line = "      "
				}
				line += opt + ", "
			}
			if strings.TrimSpace(line) != "" {
				fmt.Fprintln(w, strings.TrimSuffix(line, ", "))
			}
			if isSoft {
				fmt.Fprintln(w, "    ** WARNING: soft mount - errors returned to apps on timeout **")
			}
		}
	}
}

func writeTextReport(w io.Writer, report *Report) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, strings.Repeat("=", 75))
	fmt.Fprintln(w, "NFS MONITORING RESULTS")
	fmt.Fprintf(w, "Duration: %ds | Samples: %d | Interval: %ds\n",
		report.DurationSec, report.Samples, report.IntervalSec)
	fmt.Fprintln(w, strings.Repeat("=", 75))

	for i := range report.Mounts {
		writeTextMountSection(w, &report.Mounts[i])
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, strings.Repeat("=", 75))
}

func writeTextMountSection(w io.Writer, mr *MountReport) {
	mountpoint := mr.MountPoint
	if mountpoint == "" {
		mountpoint = "N/A"
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, strings.Repeat("─", 75))
	fmt.Fprintf(w, "MOUNT: %s (on %s)\n", mr.Device, mountpoint)
	fmt.Fprintln(w, strings.Repeat("─", 75))

	fmt.Fprintln(w, "\nSUMMARY:")
	fmt.Fprintf(w, "  Total operations: %12s  (%.1f ops/sec)\n",
		formatInt(mr.Summary.TotalOps), mr.Summary.OpsPerSec)
	fmt.Fprintf(w, "  Retransmissions:  %12s", formatInt(mr.Summary.Retrans))
	if mr.Summary.Retrans > 0 && mr.Summary.TotalOps > 0 {
		pct := float64(mr.Summary.Retrans) / float64(mr.Summary.TotalOps) * 100
		fmt.Fprintf(w, "  (%.2f%% of ops)", pct)
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  Timeouts:         %12s\n", formatInt(mr.Summary.Timeouts))
	fmt.Fprintf(w, "  Errors:           %12s\n", formatInt(mr.Summary.Errors))

	if len(mr.Operations) == 0 {
		fmt.Fprintln(w, "\n  No operations during sample period.")
		return
	}

	fmt.Fprintln(w, "\nLATENCY (ms):")
	fmt.Fprintf(w, "  %-12s %10s %8s %9s %9s %9s\n",
		"Operation", "Ops", "Ops/s", "RTT avg", "RTT min", "RTT max")
	fmt.Fprintf(w, "  %s %s %s %s %s %s\n",
		strings.Repeat("-", 12), strings.Repeat("-", 10), strings.Repeat("-", 8),
		strings.Repeat("-", 9), strings.Repeat("-", 9), strings.Repeat("-", 9))

	for _, op := range mr.Operations {
		fmt.Fprintf(w, "  %-12s %10s %8.1f %9.2f %9.2f %9.2f\n",
			op.Name, formatInt(op.Ops), op.OpsPerSec,
			op.RttAvgMs, op.RttMinMs, op.RttMaxMs)
	}

	writeBreakdownTable(w, "RETRANSMISSIONS BY OPERATION", mr.Operations,
		func(o *OpReport) int64 { return o.Retrans }, true)
	writeBreakdownTable(w, "TIMEOUTS BY OPERATION", mr.Operations,
		func(o *OpReport) int64 { return o.Timeouts }, false)
	writeBreakdownTable(w, "ERRORS BY OPERATION", mr.Operations,
		func(o *OpReport) int64 { return o.Errors }, false)
}

func writeBreakdownTable(w io.Writer, title string, ops []OpReport, accessor func(*OpReport) int64, showPct bool) {
	type row struct {
		name  string
		count int64
		total int64
	}
	var rows []row
	for i := range ops {
		c := accessor(&ops[i])
		if c > 0 {
			rows = append(rows, row{ops[i].Name, c, ops[i].Ops})
		}
	}
	if len(rows) == 0 {
		return
	}

	fmt.Fprintf(w, "\n%s:\n", title)
	if showPct {
		fmt.Fprintf(w, "  %-12s %10s %10s\n", "Operation", "Count", "% of ops")
		fmt.Fprintf(w, "  %s %s %s\n",
			strings.Repeat("-", 12), strings.Repeat("-", 10), strings.Repeat("-", 10))
		for _, r := range rows {
			pct := float64(r.count) / float64(r.total) * 100
			fmt.Fprintf(w, "  %-12s %10s %9.2f%%\n", r.name, formatInt(r.count), pct)
		}
		return
	}
	fmt.Fprintf(w, "  %-12s %10s\n", "Operation", "Count")
	fmt.Fprintf(w, "  %s %s\n", strings.Repeat("-", 12), strings.Repeat("-", 10))
	for _, r := range rows {
		fmt.Fprintf(w, "  %-12s %10s\n", r.name, formatInt(r.count))
	}
}

// ---------------------------------------------------------------------------
// Compare subcommand
// ---------------------------------------------------------------------------

func runCompare(args []string) {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: nfs-monitor compare <file1> <file2> [label1] [label2]")
		fmt.Fprintln(os.Stderr, "Example: nfs-monitor compare vast-run.json hnas-run.json VAST HNAS")
		os.Exit(1)
	}

	file1, file2 := args[0], args[1]
	label1, label2 := "File1", "File2"
	if len(args) > 2 {
		label1 = args[2]
	}
	if len(args) > 3 {
		label2 = args[3]
	}

	r1, err := loadReport(file1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", file1, err)
		os.Exit(1)
	}
	r2, err := loadReport(file2)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", file2, err)
		os.Exit(1)
	}

	m1 := primaryMount(r1, file1)
	m2 := primaryMount(r2, file2)

	printComparison(os.Stdout, r1, r2, m1, m2, label1, label2)
}

// primaryMount returns the first mount from a report, warning on stderr
// if the report contains more than one. Exits if it is empty.
func primaryMount(r *Report, path string) *MountReport {
	if len(r.Mounts) == 0 {
		fmt.Fprintf(os.Stderr, "Error: %s contains no mounts\n", path)
		os.Exit(1)
	}
	if len(r.Mounts) > 1 {
		fmt.Fprintf(os.Stderr, "Note: %s contains %d mounts; comparing the first (%s).\n",
			path, len(r.Mounts), r.Mounts[0].Device)
	}
	return &r.Mounts[0]
}

func printComparison(w io.Writer, r1, r2 *Report, m1, m2 *MountReport, label1, label2 string) {
	mount1, mount2 := m1.Device, m2.Device
	if mount1 == "" {
		mount1 = "N/A"
	}
	if mount2 == "" {
		mount2 = "N/A"
	}

	fmt.Fprintln(w, strings.Repeat("=", 80))
	fmt.Fprintln(w, "NFS Performance Comparison")
	fmt.Fprintln(w, strings.Repeat("=", 80))
	fmt.Fprintln(w)
	fmt.Fprintf(w, "%-20s %-25s %-25s\n", "", label1, label2)
	fmt.Fprintln(w, strings.Repeat("-", 80))
	fmt.Fprintf(w, "%-20s %-25s %-25s\n", "Mount:", mount1, mount2)
	fmt.Fprintf(w, "%-20s %-25s %-25s\n", "Duration:",
		fmt.Sprintf("%ds", r1.DurationSec), fmt.Sprintf("%ds", r2.DurationSec))
	fmt.Fprintln(w)

	// Summary
	fmt.Fprintln(w, "SUMMARY")
	fmt.Fprintln(w, strings.Repeat("-", 80))
	fmt.Fprintf(w, "%-20s %12s %12s %12s %12s\n", "Metric", label1, label2, "Ratio", "Better")
	fmt.Fprintf(w, "%-20s %12s %12s %12s %12s\n", "", "", "",
		fmt.Sprintf("(%s/%s)", label2, label1), "")
	fmt.Fprintln(w, strings.Repeat("-", 80))

	if m1.Summary.OpsPerSec > 0 && m2.Summary.OpsPerSec > 0 {
		ratio := m2.Summary.OpsPerSec / m1.Summary.OpsPerSec
		better := compareBetter(ratio, label1, label2, false)
		fmt.Fprintf(w, "%-20s %12.1f %12.1f %12s %12s\n", "Ops/sec",
			m1.Summary.OpsPerSec, m2.Summary.OpsPerSec, formatRatio(ratio), better)
	}
	fmt.Fprintf(w, "%-20s %12s %12s\n", "Total Ops", formatInt(m1.Summary.TotalOps), formatInt(m2.Summary.TotalOps))
	fmt.Fprintf(w, "%-20s %12s %12s\n", "Retransmissions", formatInt(m1.Summary.Retrans), formatInt(m2.Summary.Retrans))
	fmt.Fprintf(w, "%-20s %12s %12s\n", "Timeouts", formatInt(m1.Summary.Timeouts), formatInt(m2.Summary.Timeouts))
	fmt.Fprintf(w, "%-20s %12s %12s\n", "Errors", formatInt(m1.Summary.Errors), formatInt(m2.Summary.Errors))
	fmt.Fprintln(w)

	ops1 := indexOps(m1.Operations)
	ops2 := indexOps(m2.Operations)
	orderedOps := unionOpsByTotal(m1.Operations, m2.Operations)

	// Latency comparison (lower is better)
	fmt.Fprintln(w, "LATENCY COMPARISON (RTT avg in ms) - lower is better")
	fmt.Fprintln(w, strings.Repeat("-", 80))
	fmt.Fprintf(w, "%-12s %10s %10s %12s %12s\n", "Operation", label1, label2, "Ratio", "Better")
	fmt.Fprintf(w, "%-12s %10s %10s %12s %12s\n", "", "(ms)", "(ms)",
		fmt.Sprintf("(%s/%s)", label2, label1), "")
	fmt.Fprintln(w, strings.Repeat("-", 80))
	for _, name := range orderedOps {
		v1 := opField(ops1[name], func(o *OpReport) float64 { return o.RttAvgMs })
		v2 := opField(ops2[name], func(o *OpReport) float64 { return o.RttAvgMs })
		writeCompareRow(w, name, v1, v2, label1, label2, true, 2)
	}
	fmt.Fprintln(w)

	// Ops/sec comparison (higher is better)
	fmt.Fprintln(w, "OPS/SEC BY OPERATION - higher is better")
	fmt.Fprintln(w, strings.Repeat("-", 80))
	fmt.Fprintf(w, "%-12s %10s %10s %12s %12s\n", "Operation", label1, label2, "Ratio", "Better")
	fmt.Fprintf(w, "%-12s %10s %10s %12s %12s\n", "", "(ops/s)", "(ops/s)",
		fmt.Sprintf("(%s/%s)", label2, label1), "")
	fmt.Fprintln(w, strings.Repeat("-", 80))
	for _, name := range orderedOps {
		v1 := opField(ops1[name], func(o *OpReport) float64 { return o.OpsPerSec })
		v2 := opField(ops2[name], func(o *OpReport) float64 { return o.OpsPerSec })
		writeCompareRow(w, name, v1, v2, label1, label2, false, 1)
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, strings.Repeat("=", 80))
	fmt.Fprintf(w, "All ratios: (%s / %s)\n", label2, label1)
	fmt.Fprintf(w, "  Latency:  < 1 means %s is faster, > 1 means %s is faster\n", label2, label1)
	fmt.Fprintf(w, "  Ops/sec:  > 1 means %s is faster, < 1 means %s is faster\n", label2, label1)
	fmt.Fprintln(w, strings.Repeat("=", 80))
}

// indexOps returns a map of operation name -> pointer into the slice for O(1) lookup.
func indexOps(ops []OpReport) map[string]*OpReport {
	out := make(map[string]*OpReport, len(ops))
	for i := range ops {
		out[ops[i].Name] = &ops[i]
	}
	return out
}

// unionOpsByTotal returns operation names from both slices, sorted by combined ops desc.
func unionOpsByTotal(a, b []OpReport) []string {
	totals := make(map[string]int64)
	for _, op := range a {
		totals[op.Name] += op.Ops
	}
	for _, op := range b {
		totals[op.Name] += op.Ops
	}
	names := make([]string, 0, len(totals))
	for n := range totals {
		names = append(names, n)
	}
	sort.Slice(names, func(i, j int) bool { return totals[names[i]] > totals[names[j]] })
	return names
}

// opField reads a field from an OpReport pointer, returning 0 if the pointer is nil.
func opField(op *OpReport, fn func(*OpReport) float64) float64 {
	if op == nil {
		return 0
	}
	return fn(op)
}

// compareBetter returns a short arrow label indicating which side wins.
// For latency (lowerIsBetter=true), ratio < 1 means label2 wins.
// For throughput (lowerIsBetter=false), ratio > 1 means label2 wins.
func compareBetter(ratio float64, label1, label2 string, lowerIsBetter bool) string {
	switch {
	case ratio == 1:
		return "="
	case lowerIsBetter && ratio < 1, !lowerIsBetter && ratio > 1:
		return fmt.Sprintf("<- %s", label2)
	default:
		return fmt.Sprintf("%s ->", label1)
	}
}

// writeCompareRow prints one row of a comparison table, handling missing values
// on either side by showing "-" in place of the value and ratio.
func writeCompareRow(w io.Writer, name string, v1, v2 float64, label1, label2 string, lowerIsBetter bool, precision int) {
	switch {
	case v1 > 0 && v2 > 0:
		ratio := v2 / v1
		better := compareBetter(ratio, label1, label2, lowerIsBetter)
		fmt.Fprintf(w, "%-12s %10.*f %10.*f %12s %12s\n",
			name, precision, v1, precision, v2, formatRatio(ratio), better)
	case v1 > 0:
		fmt.Fprintf(w, "%-12s %10.*f %10s %12s %12s\n",
			name, precision, v1, "-", "-", "-")
	case v2 > 0:
		fmt.Fprintf(w, "%-12s %10s %10.*f %12s %12s\n",
			name, "-", precision, v2, "-", "-")
	}
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

func parseInt(s string) int64 {
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

func formatInt(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	s := fmt.Sprintf("%d", n)
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

func formatRatio(val float64) string {
	return fmt.Sprintf("%.2fx", val)
}

// sortedKeys returns the keys of a map sorted alphabetically.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
