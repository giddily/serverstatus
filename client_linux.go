//go:build linux

package main

import (
	"bufio"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	lastNet   = make(map[string]map[string]int64)
	lastNetMu sync.Mutex
)

func readNetInterfaces() ([]NetInterface, int64, int64) {
	file, err := os.Open("/proc/net/dev")
	if err != nil {
		return nil, 0, 0
	}
	defer file.Close()

	now := time.Now().UnixMilli()
	var totalUp, totalDown int64
	var nets []NetInterface

	lastNetMu.Lock()
	defer lastNetMu.Unlock()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		name := strings.TrimSpace(parts[0])
		if name == "lo" {
			continue
		}

		fields := strings.Fields(parts[1])
		if len(fields) < 10 {
			continue
		}
		rx, _ := strconv.ParseInt(fields[0], 10, 64)
		tx, _ := strconv.ParseInt(fields[8], 10, 64)

		var upRate, downRate int64
		if prev, ok := lastNet[name]; ok {
			dt := now - prev["time"]
			if dt > 0 && dt < 5000 {
				downRate = (rx - prev["rx"]) * 1000 / dt
				upRate = (tx - prev["tx"]) * 1000 / dt
				if downRate < 0 {
					downRate = 0
				}
				if upRate < 0 {
					upRate = 0
				}
			}
		}
		lastNet[name] = map[string]int64{"rx": rx, "tx": tx, "time": now}

		totalUp += upRate
		totalDown += downRate

		nets = append(nets, NetInterface{
			Name:         name,
			Upload:       tx,
			Download:     rx,
			UploadRate:   upRate,
			DownloadRate: downRate,
		})
	}
	return nets, totalUp, totalDown
}

var (
	lastCPUIdle  int64
	lastCPUTotal int64
	cpuOnce      sync.Once
)

func readCPU() float64 {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0
	}
	line := strings.SplitN(string(data), "\n", 2)[0]
	fields := strings.Fields(line)
	if len(fields) < 5 {
		return 0
	}

	var total int64
	var idle int64
	for i, f := range fields[1:] {
		v, _ := strconv.ParseInt(f, 10, 64)
		total += v
		if i == 3 {
			idle = v
		}
	}

	cpuOnce.Do(func() {
		lastCPUIdle = idle
		lastCPUTotal = total
	})

	dTotal := total - lastCPUTotal
	dIdle := idle - lastCPUIdle
	lastCPUTotal = total
	lastCPUIdle = idle

	if dTotal == 0 {
		return 0
	}
	return float64(dTotal-dIdle) / float64(dTotal) * 100
}

func readMemory() (usedPercent float64, total int64, used int64, swapTotal int64, swapUsed int64) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return
	}
	defer file.Close()

	info := make(map[string]int64)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			val = strings.TrimSuffix(val, " kB")
			v, _ := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
			info[key] = v
		}
	}

	total = info["MemTotal"]
	avail := info["MemAvailable"]
	used = total - avail
	swapTotal = info["SwapTotal"]
	swapUsed = swapTotal - info["SwapFree"]

	if total > 0 {
		usedPercent = float64(used) / float64(total) * 100
	}
	return
}

func readDisk() (usedPercent float64, total int64, used int64) {
	out, err := exec.Command("df", "-B1", "/").Output()
	if err != nil {
		return
	}
	lines := strings.Split(string(out), "\n")
	if len(lines) < 2 {
		return
	}
	fields := strings.Fields(lines[1])
	if len(fields) < 3 {
		return
	}
	total, _ = strconv.ParseInt(fields[1], 10, 64)
	used, _ = strconv.ParseInt(fields[2], 10, 64)
	if total > 0 {
		usedPercent = float64(used) / float64(total) * 100
	}
	return
}

func readLoad() (float64, float64, float64) {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, 0, 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return 0, 0, 0
	}
	l1, _ := strconv.ParseFloat(fields[0], 64)
	l5, _ := strconv.ParseFloat(fields[1], 64)
	l15, _ := strconv.ParseFloat(fields[2], 64)
	return l1, l5, l15
}

func readUptime() int64 {
	data, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 1 {
		return 0
	}
	v, _ := strconv.ParseFloat(fields[0], 64)
	return int64(v)
}

func readTCPCount() int {
	data, err := os.ReadFile("/proc/net/tcp")
	if err != nil {
		return 0
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) > 0 {
		return len(lines) - 2
	}
	return 0
}

func readTopProcesses() (topCPU []Process, topMem []Process) {
	cmd := exec.Command("ps", "aux", "--sort=-pcpu")
	out, err := cmd.Output()
	if err != nil {
		return
	}

	lines := strings.Split(string(out), "\n")
	var cpuList, memList []Process

	for i, line := range lines {
		if i == 0 || len(line) == 0 {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 11 {
			continue
		}
		cpu, _ := strconv.ParseFloat(fields[2], 64)
		mem, _ := strconv.ParseFloat(fields[3], 64)
		pid, _ := strconv.Atoi(fields[1])

		if pid == os.Getpid() || pid == 1 {
			continue
		}

		p := Process{
			Name:   fields[10],
			PID:    pid,
			CPU:    cpu,
			Memory: mem,
		}
		cpuList = append(cpuList, p)
		memList = append(memList, p)
		if len(cpuList) >= 20 {
			break
		}
	}

	sort.Slice(cpuList, func(i, j int) bool { return cpuList[i].CPU > cpuList[j].CPU })
	if len(cpuList) > 5 {
		cpuList = cpuList[:5]
	}
	topCPU = cpuList

	sort.Slice(memList, func(i, j int) bool { return memList[i].Memory > memList[j].Memory })
	if len(memList) > 5 {
		memList = memList[:5]
	}
	topMem = memList

	return
}
