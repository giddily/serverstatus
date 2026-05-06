//go:build darwin

package main

import (
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	psnet "github.com/shirou/gopsutil/v3/net"
)

var (
	lastNet    = make(map[string]map[string]int64)
	lastNetMu  sync.Mutex
	lastCPUTim cpu.TimesStat
	cpuTimInit bool
)

func readNetInterfaces() ([]NetInterface, int64, int64) {
	counters, err := psnet.IOCounters(true)
	if err != nil {
		return nil, 0, 0
	}
	now := time.Now().UnixMilli()
	var totalUp, totalDown int64
	var nets []NetInterface

	lastNetMu.Lock()
	defer lastNetMu.Unlock()

	for _, c := range counters {
		name := c.Name
		if name == "lo0" || strings.HasPrefix(name, "lo") {
			continue
		}
		rx := int64(c.BytesRecv)
		tx := int64(c.BytesSent)

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

func readCPU() float64 {
	times, err := cpu.Times(false)
	if err != nil || len(times) == 0 {
		return 0
	}
	cur := times[0]

	if !cpuTimInit {
		lastCPUTim = cur
		cpuTimInit = true
		return 0
	}

	prev := lastCPUTim
	lastCPUTim = cur

	dUser := cur.User - prev.User
	dNice := cur.Nice - prev.Nice
	dSystem := cur.System - prev.System
	dIdle := cur.Idle - prev.Idle
	dIowait := cur.Iowait - prev.Iowait
	dIrq := cur.Irq - prev.Irq
	dSoft := cur.Softirq - prev.Softirq
	dSteal := cur.Steal - prev.Steal

	idle := dIdle + dIowait
	busy := dUser + dNice + dSystem + dIrq + dSoft + dSteal
	total := idle + busy
	if total <= 0 {
		return 0
	}
	return float64(busy) / float64(total) * 100
}

func readMemory() (usedPercent float64, total int64, used int64, swapTotal int64, swapUsed int64) {
	v, err := mem.VirtualMemory()
	if err != nil {
		return
	}
	// 与 Linux /proc/meminfo 一致：字段为 kB，前端按 kB*1024 显示
	total = int64(v.Total / 1024)
	used = int64(v.Used / 1024)
	usedPercent = v.UsedPercent
	swapTotal = int64(v.SwapTotal / 1024)
	swapUsed = int64((v.SwapTotal - v.SwapFree) / 1024)
	return
}

func readDisk() (usedPercent float64, total int64, used int64) {
	u, err := disk.Usage("/")
	if err != nil {
		return
	}
	total = int64(u.Total)
	used = int64(u.Used)
	usedPercent = u.UsedPercent
	return
}

func readLoad() (float64, float64, float64) {
	a, err := load.Avg()
	if err != nil {
		return 0, 0, 0
	}
	return a.Load1, a.Load5, a.Load15
}

func readUptime() int64 {
	u, err := host.Uptime()
	if err != nil {
		return 0
	}
	return int64(u)
}

func readTCPCount() int {
	conns, err := psnet.Connections("tcp")
	if err != nil {
		return 0
	}
	return len(conns)
}

func readTopProcesses() (topCPU []Process, topMem []Process) {
	cmd := exec.Command("ps", "-axo", "pid,pcpu,pmem,comm")
	out, err := cmd.Output()
	if err != nil {
		return
	}

	lines := strings.Split(string(out), "\n")
	var list []Process

	for i, line := range lines {
		if i == 0 || len(strings.TrimSpace(line)) == 0 {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		pid, _ := strconv.Atoi(fields[0])
		cpuPct, _ := strconv.ParseFloat(fields[1], 64)
		memPct, _ := strconv.ParseFloat(fields[2], 64)
		name := strings.Join(fields[3:], " ")

		if pid == os.Getpid() || pid == 1 {
			continue
		}

		list = append(list, Process{
			Name:   name,
			PID:    pid,
			CPU:    cpuPct,
			Memory: memPct,
		})
	}

	cpuList := append([]Process(nil), list...)
	sort.Slice(cpuList, func(i, j int) bool { return cpuList[i].CPU > cpuList[j].CPU })
	if len(cpuList) > 5 {
		topCPU = cpuList[:5]
	} else {
		topCPU = cpuList
	}

	memList := append([]Process(nil), list...)
	sort.Slice(memList, func(i, j int) bool { return memList[i].Memory > memList[j].Memory })
	if len(memList) > 5 {
		topMem = memList[:5]
	} else {
		topMem = memList
	}

	return
}
