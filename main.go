package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

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

// AggregatedOp holds aggregated statistics for an operation.
type AggregatedOp struct {
	OpsTotal       int64
	RetransTotal   int64
	TimeoutsTotal  int64
	ErrorsTotal    int64
	RttWeightedMs  int64
	ExecWeightedMs int64
	RttSamples     []float64
}

// opEntry pairs an operation name with its aggregated data for sorting.
type opEntry struct {
	name string
	data *AggregatedOp
}

// breakdownEntry holds a single row for the breakdown tables.
type breakdownEntry struct {
	name  string
	count int64
	ops   int64
}

// Package-level compiled regexes for mountstats parsing.
var (
	deviceRe = regexp.MustCompile(`^device\s+(\S+)\s+mounted on\s+(\S+)\s+with fstype\s+(\S+)`)
	opRe     = regexp.MustCompile(`^\s*(\w+):\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*(\d+)?`)
)

type stringSlice []string

func (s *stringSlice) String() string {
	return strings.Join(*s, ", ")
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	// Dispatch to compare subcommand if requested.
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
	)

	flag.Var(&mountpoints, "mp", "NFS mountpoint to monitor (can specify multiple)")
	flag.BoolVar(&allMounts, "a", false, "Monitor all NFS mounts")
	flag.BoolVar(&allMounts, "all", false, "Monitor all NFS mounts")
	flag.IntVar(&duration, "d", 60, "Monitoring duration in seconds")
	flag.IntVar(&duration, "duration", 60, "Monitoring duration in seconds")
	flag.IntVar(&interval, "i", 1, "Sample interval in seconds")
	flag.IntVar(&interval, "interval", 1, "Sample interval in seconds")
	flag.BoolVar(&listMounts, "list", false, "List available NFS mounts and exit")

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
		fmt.Fprintf(os.Stderr, "  --list                List available NFS mounts and exit\n")
		fmt.Fprintf(os.Stderr, "\nSubcommands:\n")
		fmt.Fprintf(os.Stderr, "  compare               Compare two nfs-monitor output files\n")
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
		for device, info := range allMountStats {
			fmt.Printf("  %s (on %s)\n", device, info.MountPoint)
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
			found := false
			if info, ok := allMountStats[target]; ok {
				mounts[target] = info
				found = true
			} else {
				for device, info := range allMountStats {
					if info.MountPoint == target {
						mounts[device] = info
						found = true
						break
					}
				}
			}
			if !found {
				missing = append(missing, target)
			}
		}

		if len(missing) > 0 {
			fmt.Fprintf(os.Stderr, "Error: Device(s) or Mountpoint(s) not found: %s\n", strings.Join(missing, ", "))
			fmt.Fprintln(os.Stderr, "\nAvailable NFS devices and their mountpoints:")
			for device, info := range allMountStats {
				fmt.Fprintf(os.Stderr, "  %s (on %s)\n", device, info.MountPoint)
			}
			os.Exit(1)
		}
	}

	if len(mounts) == 0 {
		fmt.Fprintln(os.Stderr, "Error: No matching NFS mounts found for monitoring.")
		os.Exit(1)
	}

	printHeader()
	printMountInfo(mounts)

	fmt.Printf("\nStarting %ds monitoring (interval: %ds)...\n", duration, interval)
	fmt.Println("Run your workload now.")
	fmt.Println(strings.Repeat("-", 40))

	samples := collectSamples(duration, interval, mounts)
	if len(samples) == 0 {
		fmt.Println("No samples collected.")
		os.Exit(1)
	}

	aggregated := aggregateSamples(samples)

	targetDevices := make(map[string]string)
	for device, info := range mounts {
		mp := info.MountPoint
		if mp == "" {
			mp = "N/A"
		}
		targetDevices[device] = mp
	}

	printReport(aggregated, duration, len(samples), interval, targetDevices)
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
				fmt.Printf("\nWarning: Mountpoint for device %s changed from %s to %s\n",
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
				fmt.Printf("\nWarning: Device %s (at %s) is no longer mounted.\n", device, mountpoint)
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
					fmt.Printf("\nWarning: Counter reset detected for op '%s' on mount '%s'. Skipping sample.\n", opName, mountpoint)
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
		fmt.Printf("\r  Sampling... %3ds / %ds  (%d samples)", elapsed, durationSec, sampleNum)
	}

	fmt.Println()

	if resetCount > 0 {
		fmt.Printf("  Note: %d counter resets detected (likely due to remount or stats reset).\n", resetCount)
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
				if agg[device][opName] == nil {
					agg[device][opName] = &AggregatedOp{}
				}

				a := agg[device][opName]
				a.OpsTotal += delta.Ops
				a.RetransTotal += delta.Retrans
				a.TimeoutsTotal += delta.Timeouts
				a.ErrorsTotal += delta.Errors
				a.RttWeightedMs += delta.RttMs
				a.ExecWeightedMs += delta.ExecMs

				if delta.Ops > 0 {
					avgRtt := float64(delta.RttMs) / float64(delta.Ops)
					a.RttSamples = append(a.RttSamples, avgRtt)
				}
			}
		}
	}

	return agg
}

// ---------------------------------------------------------------------------
// Report output
// ---------------------------------------------------------------------------

func printHeader() {
	fmt.Println(strings.Repeat("=", 75))
	fmt.Println("NFS Mount Statistics Monitor")
	fmt.Println(strings.Repeat("=", 75))
}

func printMountInfo(mounts map[string]*MountInfo) {
	fmt.Println("\nNFS Mounts to Monitor:")
	fmt.Println(strings.Repeat("-", 70))

	for device, info := range mounts {
		fmt.Printf("\n  %s (on %s)\n", device, info.MountPoint)
		fmt.Printf("    Type:   %s\n", info.FSType)

		if info.Options != "" {
			fmt.Println("    Options:")
			opts := strings.Split(info.Options, ",")
			line := "      "
			isSoft := false
			for _, opt := range opts {
				opt = strings.TrimSpace(opt)
				if opt == "soft" {
					isSoft = true
				}
				if len(line)+len(opt)+2 > 75 {
					fmt.Println(line)
					line = "      "
				}
				line += opt + ", "
			}
			if strings.TrimSpace(line) != "" {
				fmt.Println(strings.TrimSuffix(line, ", "))
			}
			if isSoft {
				fmt.Println("    ** WARNING: soft mount - errors returned to apps on timeout **")
			}
		}
	}
}

func printReport(agg map[string]map[string]*AggregatedOp, durationSec, numSamples, intervalSec int, targetDevices map[string]string) {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 75))
	fmt.Println("NFS MONITORING RESULTS")
	fmt.Printf("Duration: %ds | Samples: %d | Interval: %ds\n", durationSec, numSamples, intervalSec)
	fmt.Println(strings.Repeat("=", 75))

	for device, opsData := range agg {
		mountpoint := targetDevices[device]
		if mountpoint == "" {
			mountpoint = "N/A"
		}
		fmt.Println()
		fmt.Println(strings.Repeat("─", 75))
		fmt.Printf("MOUNT: %s (on %s)\n", device, mountpoint)
		fmt.Println(strings.Repeat("─", 75))

		var totalOps, totalRetrans, totalTimeouts, totalErrors int64
		for _, op := range opsData {
			totalOps += op.OpsTotal
			totalRetrans += op.RetransTotal
			totalTimeouts += op.TimeoutsTotal
			totalErrors += op.ErrorsTotal
		}

		opsPerSec := float64(totalOps) / float64(durationSec)

		fmt.Println("\nSUMMARY:")
		fmt.Printf("  Total operations: %12s  (%.1f ops/sec)\n", formatInt(totalOps), opsPerSec)
		fmt.Printf("  Retransmissions:  %12s", formatInt(totalRetrans))
		if totalRetrans > 0 && totalOps > 0 {
			pct := float64(totalRetrans) / float64(totalOps) * 100
			fmt.Printf("  (%.2f%% of ops)", pct)
		}
		fmt.Println()
		fmt.Printf("  Timeouts:         %12s\n", formatInt(totalTimeouts))
		fmt.Printf("  Errors:           %12s\n", formatInt(totalErrors))

		// Filter to active ops and sort by count descending.
		var sortedOps []opEntry
		for name, op := range opsData {
			if op.OpsTotal > 0 {
				sortedOps = append(sortedOps, opEntry{name, op})
			}
		}

		if len(sortedOps) == 0 {
			fmt.Println("\n  No operations during sample period.")
			continue
		}

		sort.Slice(sortedOps, func(i, j int) bool {
			return sortedOps[i].data.OpsTotal > sortedOps[j].data.OpsTotal
		})

		// Latency table
		fmt.Println("\nLATENCY (ms):")
		fmt.Printf("  %-12s %10s %8s %9s %9s %9s\n", "Operation", "Ops", "Ops/s", "RTT avg", "RTT min", "RTT max")
		fmt.Printf("  %s %s %s %s %s %s\n",
			strings.Repeat("-", 12), strings.Repeat("-", 10), strings.Repeat("-", 8),
			strings.Repeat("-", 9), strings.Repeat("-", 9), strings.Repeat("-", 9))

		for _, entry := range sortedOps {
			ops := entry.data.OpsTotal
			opsSec := float64(ops) / float64(durationSec)

			var avgRtt float64
			if ops > 0 {
				avgRtt = float64(entry.data.RttWeightedMs) / float64(ops)
			}

			var minRtt, maxRtt float64
			if len(entry.data.RttSamples) > 0 {
				minRtt = entry.data.RttSamples[0]
				maxRtt = entry.data.RttSamples[0]
				for _, v := range entry.data.RttSamples {
					if v < minRtt {
						minRtt = v
					}
					if v > maxRtt {
						maxRtt = v
					}
				}
			}

			fmt.Printf("  %-12s %10s %8.1f %9.2f %9.2f %9.2f\n",
				entry.name, formatInt(ops), opsSec, avgRtt, minRtt, maxRtt)
		}

		// Breakdown tables
		printBreakdownTable("RETRANSMISSIONS BY OPERATION",
			filterOps(sortedOps, func(a *AggregatedOp) int64 { return a.RetransTotal }), true)
		printBreakdownTable("TIMEOUTS BY OPERATION",
			filterOps(sortedOps, func(a *AggregatedOp) int64 { return a.TimeoutsTotal }), false)
		printBreakdownTable("ERRORS BY OPERATION",
			filterOps(sortedOps, func(a *AggregatedOp) int64 { return a.ErrorsTotal }), false)
	}

	fmt.Println()
	fmt.Println(strings.Repeat("=", 75))
}

func filterOps(sorted []opEntry, accessor func(*AggregatedOp) int64) []breakdownEntry {
	var entries []breakdownEntry
	for _, e := range sorted {
		count := accessor(e.data)
		if count > 0 {
			entries = append(entries, breakdownEntry{e.name, count, e.data.OpsTotal})
		}
	}
	return entries
}

func printBreakdownTable(title string, entries []breakdownEntry, showPct bool) {
	if len(entries) == 0 {
		return
	}
	fmt.Printf("\n%s:\n", title)
	if showPct {
		fmt.Printf("  %-12s %10s %10s\n", "Operation", "Count", "% of ops")
		fmt.Printf("  %s %s %s\n", strings.Repeat("-", 12), strings.Repeat("-", 10), strings.Repeat("-", 10))
		for _, e := range entries {
			pct := float64(e.count) / float64(e.ops) * 100
			fmt.Printf("  %-12s %10s %9.2f%%\n", e.name, formatInt(e.count), pct)
		}
	} else {
		fmt.Printf("  %-12s %10s\n", "Operation", "Count")
		fmt.Printf("  %s %s\n", strings.Repeat("-", 12), strings.Repeat("-", 10))
		for _, e := range entries {
			fmt.Printf("  %-12s %10s\n", e.name, formatInt(e.count))
		}
	}
}

// ---------------------------------------------------------------------------
// Compare subcommand
// ---------------------------------------------------------------------------

// compareMetrics holds parsed metrics from an nfs-monitor output file.
type compareMetrics struct {
	Mount      string
	Duration   int
	OpsSec     float64
	TotalOps   int64
	Retrans    int64
	Timeouts   int64
	Errors     int64
	Operations map[string]*compareOpMetrics
}

// compareOpMetrics holds per-operation metrics parsed from output.
type compareOpMetrics struct {
	Ops    int64
	OpsSec float64
	RttAvg float64
	RttMin float64
	RttMax float64
}

// Compiled regexes for parsing nfs-monitor text output.
var (
	cmpMountRe     = regexp.MustCompile(`MOUNT:\s+(\S+)`)
	cmpDurationRe  = regexp.MustCompile(`Duration:\s+(\d+)s`)
	cmpOpsSecRe    = regexp.MustCompile(`\(([0-9.]+)\s+ops/sec\)`)
	cmpTotalOpsRe  = regexp.MustCompile(`Total operations:\s+([0-9,]+)`)
	cmpRetransRe   = regexp.MustCompile(`Retransmissions:\s+([0-9,]+)`)
	cmpTimeoutsRe  = regexp.MustCompile(`Timeouts:\s+([0-9,]+)`)
	cmpErrorsRe    = regexp.MustCompile(`(?m)^\s+Errors:\s+([0-9,]+)`)
	cmpLatencyOpRe = regexp.MustCompile(`(?m)^\s+([A-Z]+)\s+([0-9,]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)`)
)

func runCompare(args []string) {
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: nfs-monitor compare <file1> <file2> [label1] [label2]\n")
		fmt.Fprintf(os.Stderr, "Example: nfs-monitor compare vast-run.out hnas-run.out VAST HNAS\n")
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

	m1, err := parseNFSOutput(file1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", file1, err)
		os.Exit(1)
	}
	m2, err := parseNFSOutput(file2)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", file2, err)
		os.Exit(1)
	}

	printComparison(m1, m2, label1, label2)
}

func parseNFSOutput(filepath string) (*compareMetrics, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	content := string(data)

	m := &compareMetrics{
		Operations: make(map[string]*compareOpMetrics),
	}

	if match := cmpMountRe.FindStringSubmatch(content); match != nil {
		m.Mount = match[1]
	}
	if match := cmpDurationRe.FindStringSubmatch(content); match != nil {
		m.Duration = int(parseInt(match[1]))
	}
	if match := cmpOpsSecRe.FindStringSubmatch(content); match != nil {
		m.OpsSec, _ = strconv.ParseFloat(match[1], 64)
	}
	if match := cmpTotalOpsRe.FindStringSubmatch(content); match != nil {
		m.TotalOps = parseFormattedInt(match[1])
	}
	if match := cmpRetransRe.FindStringSubmatch(content); match != nil {
		m.Retrans = parseFormattedInt(match[1])
	}
	if match := cmpTimeoutsRe.FindStringSubmatch(content); match != nil {
		m.Timeouts = parseFormattedInt(match[1])
	}
	if match := cmpErrorsRe.FindStringSubmatch(content); match != nil {
		m.Errors = parseFormattedInt(match[1])
	}

	for _, match := range cmpLatencyOpRe.FindAllStringSubmatch(content, -1) {
		opsSec, _ := strconv.ParseFloat(match[3], 64)
		rttAvg, _ := strconv.ParseFloat(match[4], 64)
		rttMin, _ := strconv.ParseFloat(match[5], 64)
		rttMax, _ := strconv.ParseFloat(match[6], 64)
		m.Operations[match[1]] = &compareOpMetrics{
			Ops:    parseFormattedInt(match[2]),
			OpsSec: opsSec,
			RttAvg: rttAvg,
			RttMin: rttMin,
			RttMax: rttMax,
		}
	}

	return m, nil
}

func printComparison(m1, m2 *compareMetrics, label1, label2 string) {
	mount1, mount2 := m1.Mount, m2.Mount
	if mount1 == "" {
		mount1 = "N/A"
	}
	if mount2 == "" {
		mount2 = "N/A"
	}

	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("NFS Performance Comparison")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println()
	fmt.Printf("%-20s %-25s %-25s\n", "", label1, label2)
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("%-20s %-25s %-25s\n", "Mount:", mount1, mount2)
	fmt.Printf("%-20s %-25s %-25s\n", "Duration:",
		fmt.Sprintf("%ds", m1.Duration), fmt.Sprintf("%ds", m2.Duration))
	fmt.Println()

	// Summary
	fmt.Println("SUMMARY")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("%-20s %12s %12s %12s %12s\n", "Metric", label1, label2, "Ratio", "Better")
	fmt.Printf("%-20s %12s %12s %12s %12s\n", "", "", "", fmt.Sprintf("(%s/%s)", label2, label1), "")
	fmt.Println(strings.Repeat("-", 80))

	if m1.OpsSec > 0 && m2.OpsSec > 0 {
		ratio := m2.OpsSec / m1.OpsSec
		better := "="
		if ratio > 1 {
			better = fmt.Sprintf("<- %s", label2)
		} else if ratio < 1 {
			better = fmt.Sprintf("%s ->", label1)
		}
		fmt.Printf("%-20s %12.1f %12.1f %12s %12s\n", "Ops/sec", m1.OpsSec, m2.OpsSec, formatRatio(ratio), better)
	}

	fmt.Printf("%-20s %12s %12s\n", "Total Ops", formatInt(m1.TotalOps), formatInt(m2.TotalOps))
	fmt.Printf("%-20s %12s %12s\n", "Retransmissions", formatInt(m1.Retrans), formatInt(m2.Retrans))
	fmt.Printf("%-20s %12s %12s\n", "Timeouts", formatInt(m1.Timeouts), formatInt(m2.Timeouts))
	fmt.Printf("%-20s %12s %12s\n", "Errors", formatInt(m1.Errors), formatInt(m2.Errors))
	fmt.Println()

	// Collect all operations sorted by total ops count across both files.
	allOpsSet := make(map[string]bool)
	for op := range m1.Operations {
		allOpsSet[op] = true
	}
	for op := range m2.Operations {
		allOpsSet[op] = true
	}
	var allOps []string
	for op := range allOpsSet {
		allOps = append(allOps, op)
	}
	sort.Slice(allOps, func(i, j int) bool {
		ci := int64(0)
		cj := int64(0)
		if o, ok := m1.Operations[allOps[i]]; ok {
			ci += o.Ops
		}
		if o, ok := m2.Operations[allOps[i]]; ok {
			ci += o.Ops
		}
		if o, ok := m1.Operations[allOps[j]]; ok {
			cj += o.Ops
		}
		if o, ok := m2.Operations[allOps[j]]; ok {
			cj += o.Ops
		}
		return ci > cj
	})

	// Latency comparison
	fmt.Println("LATENCY COMPARISON (RTT avg in ms) - lower is better")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("%-12s %10s %10s %12s %12s\n", "Operation", label1, label2, "Ratio", "Better")
	fmt.Printf("%-12s %10s %10s %12s %12s\n", "", "(ms)", "(ms)", fmt.Sprintf("(%s/%s)", label2, label1), "")
	fmt.Println(strings.Repeat("-", 80))

	for _, op := range allOps {
		op1 := m1.Operations[op]
		op2 := m2.Operations[op]
		rtt1, rtt2 := float64(0), float64(0)
		if op1 != nil {
			rtt1 = op1.RttAvg
		}
		if op2 != nil {
			rtt2 = op2.RttAvg
		}

		if rtt1 > 0 && rtt2 > 0 {
			ratio := rtt2 / rtt1
			better := "="
			if ratio < 1 {
				better = fmt.Sprintf("<- %s", label2)
			} else if ratio > 1 {
				better = fmt.Sprintf("%s ->", label1)
			}
			fmt.Printf("%-12s %10.2f %10.2f %12s %12s\n", op, rtt1, rtt2, formatRatio(ratio), better)
		} else if rtt1 > 0 {
			fmt.Printf("%-12s %10.2f %10s %12s %12s\n", op, rtt1, "-", "-", "-")
		} else if rtt2 > 0 {
			fmt.Printf("%-12s %10s %10.2f %12s %12s\n", op, "-", rtt2, "-", "-")
		}
	}
	fmt.Println()

	// Ops/sec comparison
	fmt.Println("OPS/SEC BY OPERATION - higher is better")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("%-12s %10s %10s %12s %12s\n", "Operation", label1, label2, "Ratio", "Better")
	fmt.Printf("%-12s %10s %10s %12s %12s\n", "", "(ops/s)", "(ops/s)", fmt.Sprintf("(%s/%s)", label2, label1), "")
	fmt.Println(strings.Repeat("-", 80))

	for _, op := range allOps {
		op1 := m1.Operations[op]
		op2 := m2.Operations[op]
		sec1, sec2 := float64(0), float64(0)
		if op1 != nil {
			sec1 = op1.OpsSec
		}
		if op2 != nil {
			sec2 = op2.OpsSec
		}

		if sec1 > 0 && sec2 > 0 {
			ratio := sec2 / sec1
			better := "="
			if ratio > 1 {
				better = fmt.Sprintf("<- %s", label2)
			} else if ratio < 1 {
				better = fmt.Sprintf("%s ->", label1)
			}
			fmt.Printf("%-12s %10.1f %10.1f %12s %12s\n", op, sec1, sec2, formatRatio(ratio), better)
		} else if sec1 > 0 {
			fmt.Printf("%-12s %10.1f %10s %12s %12s\n", op, sec1, "-", "-", "-")
		} else if sec2 > 0 {
			fmt.Printf("%-12s %10s %10.1f %12s %12s\n", op, "-", sec2, "-", "-")
		}
	}

	fmt.Println()
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("All ratios: (%s / %s)\n", label2, label1)
	fmt.Println("  Latency:  < 1 means", label2, "is faster, > 1 means", label1, "is faster")
	fmt.Println("  Ops/sec:  > 1 means", label2, "is faster, < 1 means", label1, "is faster")
	fmt.Println(strings.Repeat("=", 80))
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

func parseInt(s string) int64 {
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

func parseFormattedInt(s string) int64 {
	return parseInt(strings.ReplaceAll(s, ",", ""))
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
