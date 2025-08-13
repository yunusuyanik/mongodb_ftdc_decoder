package main

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"html"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

/* ============================== Flags & Types ============================== */

var (
	flagDir      string
	flagWebDir   string
	flagDebug    bool
	fileLogger   *log.Logger
)

type Point struct {
	T       time.Time          `json:"t"`
	Metrics map[string]float64 `json:"metrics"`
}

// ApiPayload struct updated to include version information
type ApiPayload struct {
	Hostname string               `json:"hostname"`
	Version  string               `json:"version"` // Added version field
	Labels   []string             `json:"labels"`
	Series   map[string][]float64 `json:"series"`
	Keys     []string             `json:"keys"`
	Groups   map[string][]string  `json:"groups"`
}

func dbg(msg string, a ...interface{}) {
	if flagDebug {
		log.Printf("[DEBUG] "+msg, a...)
	}
}

/* =================================== main ================================= */

func main() {
	flag.StringVar(&flagDir, "dir", "", "Path to diagnostic.data (folder containing metrics.* files)")
	flag.StringVar(&flagWebDir, "web", "./web", "Static web folder (must include index.html)")
	flag.BoolVar(&flagDebug, "debug", false, "Enable debug logs")
	flag.Parse()

	logFile, err := os.OpenFile("ftdc_utilization.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Log file could not be opened: %v", err)
	}
	defer logFile.Close()
	fileLogger = log.New(logFile, "", log.LstdFlags)
	fileLogger.Println("--- Logging started ---")

	if flagDir == "" {
		log.Fatal("set -dir to the diagnostic.data folder")
	}

	files, err := findMetricFiles(flagDir)
	if err != nil {
		log.Fatal(err)
	}
	if len(files) == 0 {
		log.Fatalf("no metrics.* files found under %s", flagDir)
	}

	// extractAll now returns version information
	points, host, version, err := extractAll(files)
	if err != nil {
		log.Fatal(err)
	}
	if len(points) == 0 {
		log.Fatal("no points extracted (could not parse timestamp/metrics)")
	}

	sort.Slice(points, func(i, j int) bool { return points[i].T.Before(points[j].T) })

	labels := make([]string, len(points))
	series := map[string][]float64{}
	seen := map[string]struct{}{}

	for i, p := range points {
		labels[i] = p.T.Format("2006-01-02 15:04")
		for k, v := range p.Metrics {
			if _, ok := series[k]; !ok {
				series[k] = make([]float64, len(points))
			}
			series[k][i] = v
			seen[k] = struct{}{}
		}
	}

	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	groups := map[string][]string{}
	for _, k := range keys {
		g := groupOf(k)
		groups[g] = append(groups[g], k)
	}

	// Payload now includes the version
	payload := &ApiPayload{
		Hostname: host,
		Version:  version,
		Labels:   labels,
		Series:   series,
		Keys:     keys,
		Groups:   groups,
	}

	http.HandleFunc("/api/data", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(payload)
	})
	http.Handle("/", http.FileServer(http.Dir(flagWebDir)))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	url := "http://" + ln.Addr().String() + "/"
	log.Printf("Dashboard: %s", url)

	go func() {
		if err := http.Serve(ln, nil); err != nil {
			log.Fatal(err)
		}
	}()
	if runtime.GOOS == "darwin" {
		_ = exec.Command("open", url).Start()
	}

	select {}
}

/* ============================== FTDC Extraction ============================ */

// extractAll now returns the version as well
func extractAll(files []string) ([]Point, string, string, error) {
	var out []Point
	var host, version string
	var prev Point

	for _, f := range files {
		dbg("bsondump %s", filepath.Base(f))
		cmd := exec.Command("bsondump", "--quiet", f)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return nil, "", "", err
		}
		if err := cmd.Start(); err != nil {
			return nil, "", "", err
		}

		dec := json.NewDecoder(stdout)
		for {
			var obj map[string]any
			if err := dec.Decode(&obj); err != nil {
				if err == io.EOF {
					break
				}
				return nil, "", "", err
			}
			if getNum(obj["type"]) != 1 {
				continue
			}
			b64 := extractBase64(obj)
			if b64 == "" {
				continue
			}
			raw, err := base64.StdEncoding.DecodeString(b64)
			if err != nil || len(raw) <= 4 {
				continue
			}
			zr, err := zlib.NewReader(bytes.NewReader(raw[4:]))
			if err != nil {
				continue
			}
			inflated, _ := io.ReadAll(zr)
			_ = zr.Close()
			if len(inflated) == 0 {
				continue
			}

			for _, bs := range splitConcatBSON(inflated) {
				var m bson.M
				if err := bson.Unmarshal(bs, &m); err != nil {
					continue
				}

				ts, ok := pickTimestamp(m)
				if !ok {
					dbg("skip chunk: no parsable timestamp")
					continue
				}

				// Extract hostname if not already found
				if host == "" {
					if h, ok := getNestedString(m, "serverStatus.host"); ok {
						host = h
					}
				}
				// Extract version if not already found
				if version == "" {
					if v, ok := getNestedString(m, "serverStatus.version"); ok {
						version = v
					}
				}

				rm := collectRaw(m)
				if len(rm) == 0 {
					continue
				}

				cur := Point{T: ts, Metrics: rm}
				if prev.T.IsZero() {
					prev = cur
					continue
				}
				dt := cur.T.Sub(prev.T).Seconds()
				if dt <= 0 {
					prev = cur
					continue
				}

				final := finalize(cur, prev)
				out = append(out, Point{T: ts, Metrics: final})
				prev = cur
			}
		}
		_ = cmd.Wait()
	}

	if host == "" {
		host = html.EscapeString(filepath.Base(flagDir))
	}
	return out, host, version, nil
}

/* ============================== Raw Collection ============================= */

func collectRaw(root map[string]any) map[string]float64 {
	out := map[string]float64{}

	ss, hasSS := getMap(root, "serverStatus")
	sys, hasSys := getMap(root, "systemMetrics")
	repl, hasRepl := getMap(root, "replSetGetStatus")

	if hasSS {
		if v, ok := getFloat(ss, "mem.resident"); ok {
			out["gauge_mongo_mem_resident_mb"] = v
		}
		if v, ok := getFloat(ss, "mem.virtual"); ok {
			out["gauge_mongo_mem_virtual_mb"] = v
		}
		if v, ok := getFloat(ss, "connections.current"); ok {
			out["gauge_connections_current"] = v
		}
		if v, ok := getFloat(ss, "connections.available"); ok {
			out["gauge_connections_available"] = v
		}
		if v, ok := getFloat(ss, "connections.totalCreated"); ok {
			out["counter_connections_created"] = v
		}
		if v, ok := getFloat(ss, "globalLock.currentQueue.total"); ok {
			out["gauge_globallock_queue_total"] = v
		}
		if v, ok := getFloat(ss, "globalLock.currentQueue.readers"); ok {
			out["gauge_globallock_queue_readers"] = v
		}
		if v, ok := getFloat(ss, "globalLock.currentQueue.writers"); ok {
			out["gauge_globallock_queue_writers"] = v
		}
		if v, ok := getFloat(ss, "wiredTiger.cache.bytes currently in the cache"); ok {
			out["gauge_wt_cache_bytes_in_cache_mb"] = v / (1024 * 1024)
		}
		if v, ok := getFloat(ss, "wiredTiger.cache.tracked dirty bytes in the cache"); ok {
			out["gauge_wt_cache_dirty_bytes_mb"] = v / (1024 * 1024)
		} else if v, ok := getFloat(ss, "wiredTiger.cache.tracked dirty bytes"); ok {
			out["gauge_wt_cache_dirty_bytes_mb"] = v / (1024 * 1024)
		}
		if v, ok := getFloat(ss, "wiredTiger.cache.maximum bytes configured"); ok {
			out["gauge_wt_cache_max_bytes_mb"] = v / (1024 * 1024)
		}
		if v, ok := getFloat(ss, "wiredTiger.concurrentTransactions.read.available"); ok {
			out["gauge_wt_tickets_avail_read"] = v
		}
		if v, ok := getFloat(ss, "wiredTiger.concurrentTransactions.write.available"); ok {
			out["gauge_wt_tickets_avail_write"] = v
		}
		for _, k := range []string{"query", "insert", "update", "delete", "command", "getmore"} {
			if v, ok := getFloat(ss, "opcounters."+k); ok {
				out["counter_opcounters_"+k] = v
			}
		}
		if v, ok := getFloat(ss, "metrics.document.returned"); ok {
			out["counter_docs_returned"] = v
		}
		if v, ok := getFloat(ss, "metrics.document.inserted"); ok {
			out["counter_docs_inserted"] = v
		}
		if v, ok := getFloat(ss, "metrics.document.updated"); ok {
			out["counter_docs_updated"] = v
		}
		if v, ok := getFloat(ss, "metrics.document.deleted"); ok {
			out["counter_docs_deleted"] = v
		}
		if v, ok := getFloat(ss, "opLatencies.reads.latency"); ok {
			out["counter_latency_reads_ms"] = v
		}
		if v, ok := getFloat(ss, "opLatencies.reads.ops"); ok {
			out["counter_ops_reads"] = v
		}
		if v, ok := getFloat(ss, "opLatencies.writes.latency"); ok {
			out["counter_latency_writes_ms"] = v
		}
		if v, ok := getFloat(ss, "opLatencies.writes.ops"); ok {
			out["counter_ops_writes"] = v
		}
		if v, ok := getFloat(ss, "opLatencies.commands.latency"); ok {
			out["counter_latency_commands_ms"] = v
		}
		if v, ok := getFloat(ss, "opLatencies.commands.ops"); ok {
			out["counter_ops_commands"] = v
		}
		if v, ok := getFloat(ss, "network.physicalBytesIn"); ok {
			out["counter_network_bytes_in"] = v
		} else if v, ok := getFloat(ss, "network.bytesIn"); ok {
			out["counter_network_bytes_in"] = v
		}
		if v, ok := getFloat(ss, "network.physicalBytesOut"); ok {
			out["counter_network_bytes_out"] = v
		} else if v, ok := getFloat(ss, "network.bytesOut"); ok {
			out["counter_network_bytes_out"] = v
		}
		if v, ok := getFloat(ss, "wiredTiger.block-manager.bytes read"); ok {
			out["counter_wt_bytes_read"] = v
		}
		if v, ok := getFloat(ss, "wiredTiger.block-manager.bytes written"); ok {
			out["counter_wt_bytes_written"] = v
		}
		if v, ok := getFloat(ss, "wiredTiger.cache.pages read into cache"); ok {
			out["counter_wt_pages_read_into_cache"] = v
		}
		if v, ok := getFloat(ss, "wiredTiger.cache.pages written from cache"); ok {
			out["counter_wt_pages_written_from_cache"] = v
		}
		if v, ok := getFloat(ss, "extra_info.page_faults"); ok {
			out["counter_page_faults"] = v
		}
		if v, ok := getFloatAny(ss,
			"queryExecutor.scanned",
			"metrics.queryExecutor.scanned",
		); ok {
			out["counter_query_scanned_keys"] = v
		}
		if v, ok := getFloatAny(ss,
			"queryExecutor.scannedObjects",
			"metrics.queryExecutor.scannedObjects",
		); ok {
			out["counter_query_scanned_objects"] = v
		}
		if v, ok := getFloatAny(ss,
			"queryExecutor.collectionScans.total",
			"metrics.queryExecutor.collectionScans.total",
		); ok {
			out["counter_query_collection_scans_total"] = v
		}
		if v, ok := getFloatAny(ss,
			"queryExecutor.collectionScans.nonTailable",
			"metrics.queryExecutor.collectionScans.nonTailable",
		); ok {
			out["counter_query_collection_scans_nontailable"] = v
		}
		if v, ok := getFloatAny(ss, "ttl.passes", "metrics.ttl.passes"); ok {
			out["counter_ttl_passes"] = v
		}
		if v, ok := getFloatAny(ss, "ttl.deletedDocuments", "metrics.ttl.deletedDocuments"); ok {
			out["counter_ttl_deletedDocuments"] = v
		}
	}

	if hasSys {
		if cpu, ok := getMap(sys, "cpu"); ok {
			if v, ok := getFloat(cpu, "num_cpus"); ok {
				out["gauge_cpu_num_cpus"] = v
			} else {
				out["gauge_cpu_num_cpus"] = 1
			}
			for _, k := range []string{"user_ms", "system_ms", "idle_ms", "iowait_ms", "nice_ms", "softirq_ms", "steal_ms", "guest_ms", "guest_nice_ms"} {
				if v, ok := getFloat(cpu, k); ok {
					out["counter_cpu_"+k] = v
				}
			}
		}
		if mem, ok := getMap(sys, "memory"); ok {
			if v, ok := getFloat(mem, "MemAvailable_kb"); ok {
				out["gauge_sys_mem_available_mb"] = v / 1024.0
			}
			if v, ok := getFloat(mem, "MemTotal_kb"); ok {
				out["gauge_sys_mem_total_mb"] = v / 1024.0
			}
		}
		if vm, ok := getMap(sys, "vmstat"); ok {
			if v, ok := getFloat(vm, "pgmajfault"); ok {
				out["counter_vmstat_major_faults"] = v
			}
			if v, ok := getFloat(vm, "pswpin"); ok {
				out["counter_vmstat_swap_in"] = v
			}
			if v, ok := getFloat(vm, "pswpout"); ok {
				out["counter_vmstat_swap_out"] = v
			}
		}
		if disks, ok := getMap(sys, "disks"); ok {
			for dev, raw := range disks {
				dm, ok := toMap(raw)
				if !ok {
					continue
				}
				if v, ok := getFloat(dm, "reads"); ok {
					out["counter_disk_reads_"+dev] = v
				}
				if v, ok := getFloat(dm, "writes"); ok {
					out["counter_disk_writes_"+dev] = v
				}
				if v, ok := getFloat(dm, "read_time_ms"); ok {
					out["counter_disk_read_time_ms_"+dev] = v
				}
				if v, ok := getFloat(dm, "write_time_ms"); ok {
					out["counter_disk_write_time_ms_"+dev] = v
				}
				if v, ok := getFloat(dm, "io_time_ms"); ok {
					out["counter_disk_io_time_ms_"+dev] = v
				}
				if v, ok := getFloat(dm, "io_queued_ms"); ok {
					out["counter_disk_io_queued_ms_"+dev] = v
				}
				if v, ok := getFloat(dm, "read_sectors"); ok {
					out["counter_disk_read_sectors_"+dev] = v
				}
				if v, ok := getFloat(dm, "write_sectors"); ok {
					out["counter_disk_write_sectors_"+dev] = v
				}
			}
		}
	}

	if hasRepl {
		if sec, ok := getTimeSeconds(repl, "optimes.lastAppliedWallTime"); ok {
			out["gauge_repl_lastApplied_sec"] = sec
		}
		if sec, ok := getTimeSeconds(repl, "optimes.lastCommittedWallTime"); ok {
			out["gauge_repl_lastCommitted_sec"] = sec
		}
	}

	return out
}
// Remaining functions (finalize, groupOf, etc.) are unchanged.
// They are included here for completeness.

/* ============================== Finalize (rates) =========================== */

func finalize(point Point, prevPoint Point) map[string]float64 {
	cur := point.Metrics
	prev := prevPoint.Metrics
	dt := point.T.Sub(prevPoint.T).Seconds()

	out := map[string]float64{}

	rate := func(k string) (float64, bool) {
		c, okC := cur[k]
		p, okP := prev[k]
		if !okC || !okP {
			return 0, false
		}
		d := c - p
		if d < 0 {
			d = 0
		}
		return d / dt, true
	}
	delta := func(k string) (float64, bool) {
		c, okC := cur[k]
		p, okP := prev[k]
		if !okC || !okP {
			return 0, false
		}
		return c - p, true
	}

	for k, v := range cur {
		if strings.HasPrefix(k, "gauge_") {
			out[strings.TrimPrefix(k, "gauge_")] = v
		}
	}

	for _, c := range []string{"query", "insert", "update", "delete", "command", "getmore"} {
		if v, ok := rate("counter_opcounters_" + c); ok {
			out["opcounters_"+c+"_per_sec"] = v
		}
	}
	for _, c := range []string{"returned", "inserted", "updated", "deleted"} {
		if v, ok := rate("counter_docs_" + c); ok {
			out["docs_"+c+"_per_sec"] = v
		}
	}
	for _, c := range []string{
		"vmstat_major_faults", "vmstat_swap_in", "vmstat_swap_out",
		"page_faults",
		"wt_pages_read_into_cache", "wt_pages_written_from_cache",
	} {
		if v, ok := rate("counter_" + c); ok {
			out[c+"_per_sec"] = v
		}
	}

	if v, ok := rate("counter_connections_created"); ok {
		out["connections_created_per_sec"] = v
	}
	if v, ok := rate("counter_query_scanned_keys"); ok {
		out["query_scanned_keys_per_sec"] = v
	}
	if v, ok := rate("counter_query_scanned_objects"); ok {
		out["query_scanned_objects_per_sec"] = v
	}
	if v, ok := rate("counter_query_collection_scans_total"); ok {
		out["query_collection_scans_total_per_sec"] = v
	}
	if v, ok := rate("counter_query_collection_scans_nontailable"); ok {
		out["query_collection_scans_nontailable_per_sec"] = v
	}
	if v, ok := rate("counter_ttl_passes"); ok {
		out["ttl_passes_per_sec"] = v
	}
	if v, ok := rate("counter_ttl_deletedDocuments"); ok {
		out["ttl_deletedDocuments_per_sec"] = v
	}
	if v, ok := rate("counter_network_bytes_in"); ok {
		out["network_bytes_in_per_sec"] = v / (1024 * 1024)
	}
	if v, ok := rate("counter_network_bytes_out"); ok {
		out["network_bytes_out_per_sec"] = v / (1024 * 1024)
	}
	if v, ok := rate("counter_wt_bytes_read"); ok {
		out["wt_read_mb_per_sec"] = v / (1024 * 1024)
	}
	if v, ok := rate("counter_wt_bytes_written"); ok {
		out["wt_write_mb_per_sec"] = v / (1024 * 1024)
	}

	for _, l := range []string{"reads", "writes", "commands"} {
		if ms, ok := delta("counter_latency_" + l + "_ms"); ok {
			if ops, ok := delta("counter_ops_" + l); ok && ops > 0 {
				out["latency_"+l+"_avg_ms"] = ms / ops
			}
		}
	}

	cpu := []string{"user_ms", "system_ms", "idle_ms", "iowait_ms", "nice_ms", "softirq_ms", "steal_ms", "guest_ms", "guest_nice_ms"}
	var total float64
	part := map[string]float64{}
	for _, k := range cpu {
		if v, ok := delta("counter_cpu_" + k); ok {
			part[k] = v
			total += v
		}
	}
	if total > 0 {
		for _, k := range cpu {
			out["cpu_"+strings.TrimSuffix(k, "_ms")+"_percent"] = (part[k] / total) * 100.0
		}
	}

	devs := map[string]struct{}{}
	for k := range cur {
		if strings.HasPrefix(k, "counter_disk_reads_") {
			dev := strings.TrimPrefix(k, "counter_disk_reads_")
			devs[dev] = struct{}{}
		}
	}
	const sectorBytes = 512.0
	for dev := range devs {
		if r, ok := rate("counter_disk_reads_" + dev); ok {
			out["disk_read_iops_"+dev] = r
		}
		if r, ok := rate("counter_disk_writes_" + dev); ok {
			out["disk_write_iops_"+dev] = r
		}
		if ops, ok := delta("counter_disk_reads_" + dev); ok && ops > 0 {
			if ms, ok := delta("counter_disk_read_time_ms_" + dev); ok {
				out["disk_read_latency_ms_"+dev] = ms / ops
			}
		}
		if ops, ok := delta("counter_disk_writes_" + dev); ok && ops > 0 {
			if ms, ok := delta("counter_disk_write_time_ms_" + dev); ok {
				out["disk_write_latency_ms_"+dev] = ms / ops
			}
		}
		if busy, ok := delta("counter_disk_io_time_ms_" + dev); ok {
			utilization := (busy / (dt * 1000)) * 100
			out["disk_utilization_percent_"+dev] = utilization
			fileLogger.Printf("Disk Util [timestamp: %s, dev: %s, busy_ms: %.2f, total_ms: %.2f, util_pct: %.2f%%]",
				point.T.Format(time.RFC3339), dev, busy, dt*1000, utilization)
		}
		if qms, ok := delta("counter_disk_io_queued_ms_" + dev); ok {
			if busy, ok := delta("counter_disk_io_time_ms_" + dev); ok && busy > 0 {
				out["disk_queue_depth_avg_"+dev] = qms / busy
			}
		}
		if ds, ok := delta("counter_disk_read_sectors_" + dev); ok {
			out["disk_read_mbps_"+dev] = (ds * sectorBytes) / dt / 1e6
		}
		if ds, ok := delta("counter_disk_write_sectors_" + dev); ok {
			out["disk_write_mbps_"+dev] = (ds * sectorBytes) / dt / 1e6
		}
	}

	var totReadOps, totWriteOps float64
	var totReadMs, totWriteMs float64
	var totBusyMs, totQMs float64
	var totReadSectors, totWriteSectors float64
	var nd int

	for k := range cur {
		if strings.HasPrefix(k, "counter_disk_reads_") {
			dev := strings.TrimPrefix(k, "counter_disk_reads_")
			nd++
			if v, ok := delta("counter_disk_reads_" + dev); ok {
				totReadOps += v
			}
			if v, ok := delta("counter_disk_writes_" + dev); ok {
				totWriteOps += v
			}
			if v, ok := delta("counter_disk_read_time_ms_" + dev); ok {
				totReadMs += v
			}
			if v, ok := delta("counter_disk_write_time_ms_" + dev); ok {
				totWriteMs += v
			}
			if v, ok := delta("counter_disk_io_time_ms_" + dev); ok {
				totBusyMs += v
			}
			if v, ok := delta("counter_disk_io_queued_ms_" + dev); ok {
				totQMs += v
			}
			if v, ok := delta("counter_disk_read_sectors_" + dev); ok {
				totReadSectors += v
			}
			if v, ok := delta("counter_disk_write_sectors_" + dev); ok {
				totWriteSectors += v
			}
		}
	}
	if nd > 0 && dt > 0 {
		out["disk_read_iops_total"] = totReadOps / dt
		out["disk_write_iops_total"] = totWriteOps / dt
		out["disk_read_mbps_total"] = (totReadSectors * 512.0) / dt / 1e6
		out["disk_write_mbps_total"] = (totWriteSectors * 512.0) / dt / 1e6
		if totReadOps > 0 {
			out["disk_read_latency_ms_total"] = totReadMs / totReadOps
		}
		if totWriteOps > 0 {
			out["disk_write_latency_ms_total"] = totWriteMs / totWriteOps
		}
		out["disk_utilization_percent_total"] = (totBusyMs / (dt * 1000 * float64(nd))) * 100.0
		if totBusyMs > 0 {
			out["disk_queue_depth_avg_total"] = totQMs / totBusyMs
		}
	}

	if a, ok := cur["gauge_repl_lastApplied_sec"]; ok {
		if c, ok := cur["gauge_repl_lastCommitted_sec"]; ok {
			out["repl_commit_lag_secs"] = a - c
		}
	}

	if v, ok := cur["gauge_wt_tickets_avail_read"]; ok {
		out["wt_tickets_avail_read"] = v
	}
	if v, ok := cur["gauge_wt_tickets_avail_write"]; ok {
		out["wt_tickets_avail_write"] = v
	}
	if v, ok := cur["gauge_wt_cache_max_bytes_mb"]; ok {
		out["wt_cache_max_bytes_mb"] = v
	}
	if v, ok := cur["gauge_wt_cache_bytes_in_cache_mb"]; ok {
		out["wt_cache_bytes_in_cache_mb"] = v
	}
	if v, ok := cur["gauge_wt_cache_dirty_bytes_mb"]; ok {
		out["wt_cache_dirty_bytes_mb"] = v
	}

	return out
}

/* ================================ Helpers ================================= */

func groupOf(k string) string {
	switch {
	case strings.HasPrefix(k, "cpu_"), strings.HasPrefix(k, "sys_mem_"), strings.HasPrefix(k, "vmstat_"):
		return "OS / CPU & Memory"
	case strings.HasPrefix(k, "disk_"):
		return "OS / Disks"
	case strings.HasPrefix(k, "network_"):
		return "OS / Network"
	case strings.HasPrefix(k, "connections_"):
		return "Mongo / Connections"
	case strings.HasPrefix(k, "globallock_"):
		return "Mongo / Global Lock"
	case strings.HasPrefix(k, "opcounters_"):
		return "Mongo / Opcounters"
	case strings.HasPrefix(k, "docs_"):
		return "Mongo / Documents"
	case strings.HasPrefix(k, "latency_"):
		return "Mongo / Op Latency"
	case strings.HasPrefix(k, "query_"):
		return "Mongo / Query Executor"
	case strings.HasPrefix(k, "ttl_"):
		return "Mongo / TTL"
	case strings.HasPrefix(k, "wt_cache_"):
		return "WiredTiger / Cache"
	case strings.HasPrefix(k, "wt_tickets_"):
		return "WiredTiger / Tickets"
	case strings.HasPrefix(k, "wt_read_"), strings.HasPrefix(k, "wt_write_"):
		return "WiredTiger / IO"
	case strings.HasPrefix(k, "wt_pages_"):
		return "WiredTiger / Evictions"
	case strings.HasPrefix(k, "repl_"):
		return "Replication"
	default:
		return "Other"
	}
}

func findMetricFiles(dir string) ([]string, error) {
	var out []string
	re := regexp.MustCompile(`^metrics\.\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z-\d{5}$`)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range ents {
		if !e.IsDir() && re.MatchString(e.Name()) {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(out)
	return out, nil
}

func getNum(v any) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case map[string]any:
		if s, ok := t["$numberInt"].(string); ok {
			n, _ := strconv.ParseInt(s, 10, 64)
			return n
		}
		if s, ok := t["$numberLong"].(string); ok {
			n, _ := strconv.ParseInt(s, 10, 64)
			return n
		}
	}
	return 0
}

func extractBase64(doc map[string]any) string {
	dm, ok := doc["data"].(map[string]any)
	if !ok {
		return ""
	}
	bin, ok := dm["$binary"].(map[string]any)
	if !ok {
		return ""
	}
	if s, ok := bin["base64"].(string); ok {
		return s
	}
	return ""
}

func splitConcatBSON(buf []byte) [][]byte {
	var out [][]byte
	i := 0
	for i+4 <= len(buf) {
		l := int(int32(binary.LittleEndian.Uint32(buf[i : i+4])))
		if l <= 0 || i+l > len(buf) {
			break
		}
		out = append(out, buf[i:i+l])
		i += l
	}
	return out
}

func pickTimestamp(m map[string]any) (time.Time, bool) {
	if v, ok := getNestedValue(m, "end"); ok {
		if tm, ok := asTime(v); ok {
			return tm, true
		}
	}
	if v, ok := getNestedValue(m, "start"); ok {
		if tm, ok := asTime(v); ok {
			return tm, true
		}
	}
	if v, ok := getNestedValue(m, "serverStatus.localTime"); ok {
		if tm, ok := asTime(v); ok {
			return tm, true
		}
	}
	return time.Time{}, false
}

func asTime(v any) (time.Time, bool) {
	switch t := v.(type) {
	case primitive.DateTime:
		return t.Time(), true
	case time.Time:
		return t, true
	case int64:
		return time.UnixMilli(t), true
	case float64:
		return time.UnixMilli(int64(t)), true
	case string:
		if tm, err := time.Parse(time.RFC3339, t); err == nil {
			return tm, true
		}
	case map[string]any:
		if s, ok := t["$date"].(string); ok {
			if tm, err := time.Parse(time.RFC3339, s); err == nil {
				return tm, true
			}
		}
		if m, ok := t["$date"].(map[string]any); ok {
			if s, ok := m["$numberLong"].(string); ok {
				if n, err := strconv.ParseInt(s, 10, 64); err == nil {
					return time.UnixMilli(n), true
				}
			}
		}
	}
	return time.Time{}, false
}

func getNestedValue(m map[string]any, key string) (any, bool) {
	cur := any(m)
	for _, p := range strings.Split(key, ".") {
		switch node := cur.(type) {
		case map[string]any:
			v, ok := node[p]
			if !ok {
				return nil, false
			}
			cur = v
		case primitive.M:
			v, ok := node[p]
			if !ok {
				return nil, false
			}
			cur = v
		case primitive.D:
			found := false
			for _, e := range node {
				if e.Key == p {
					cur = e.Value
					found = true
					break
				}
			}
			if !found {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return cur, true
}

func getMap(m map[string]any, key string) (map[string]any, bool) {
	v, ok := getNestedValue(m, key)
	if !ok {
		return nil, false
	}
	return toMap(v)
}

func toMap(v any) (map[string]any, bool) {
	switch t := v.(type) {
	case map[string]any:
		return t, true
	case primitive.M:
		return map[string]any(t), true
	case primitive.D:
		dst := make(map[string]any, len(t))
		for _, e := range t {
			dst[e.Key] = e.Value
		}
		return dst, true
	default:
		return nil, false
	}
}

func getNestedString(m map[string]any, key string) (string, bool) {
	if v, ok := getNestedValue(m, key); ok {
		if s, ok := v.(string); ok {
			return s, true
		}
	}
	return "", false
}

func getTimeSeconds(m map[string]any, key string) (float64, bool) {
	if v, ok := getNestedValue(m, key); ok {
		if tm, ok := asTime(v); ok {
			return float64(tm.Unix()), true
		}
	}
	return 0, false
}

func getFloat(m map[string]any, key string) (float64, bool) {
	if v, ok := getNestedValue(m, key); ok {
		switch t := v.(type) {
		case float64:
			return t, true
		case int:
			return float64(t), true
		case int32:
			return float64(t), true
		case int64:
			return float64(t), true
		case primitive.Decimal128:
			f, err := strconv.ParseFloat(t.String(), 64)
			return f, err == nil
		case map[string]any:
			if s, ok := t["$numberLong"].(string); ok {
				f, _ := strconv.ParseFloat(s, 64)
				return f, true
			}
			if s, ok := t["$numberInt"].(string); ok {
				f, _ := strconv.ParseFloat(s, 64)
				return f, true
			}
			if s, ok := t["$numberDouble"].(string); ok {
				f, _ := strconv.ParseFloat(s, 64)
				return f, true
			}
		}
	}
	return 0, false
}

func getFloatAny(m map[string]any, paths ...string) (float64, bool) {
	for _, p := range paths {
		if v, ok := getFloat(m, p); ok {
			return v, true
		}
	}
	return 0, false
}