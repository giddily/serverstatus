package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ========== 数据结构 ==========

type NetInterface struct {
	Name         string `json:"name"`
	Upload       int64  `json:"upload"`
	Download     int64  `json:"download"`
	UploadRate   int64  `json:"upload_rate"`
	DownloadRate int64  `json:"download_rate"`
}

type Process struct {
	Name      string  `json:"name"`
	PID       int     `json:"pid"`
	CPU       float64 `json:"cpu"`
	Memory    float64 `json:"memory"`
	NetUpload int64   `json:"net_upload"`
	NetDown   int64   `json:"net_down"`
}

type ServerData struct {
	Name            string         `json:"name"`
	Protocol        string         `json:"protocol"`
	CPU             float64        `json:"cpu"`
	CPUCores        int            `json:"cpu_cores"`
	Memory          float64        `json:"memory"`
	MemoryTotal     int64          `json:"memory_total"`
	MemoryUsed      int64          `json:"memory_used"`
	SwapTotal       int64          `json:"swap_total"`
	SwapUsed        int64          `json:"swap_used"`
	Disk            float64        `json:"disk"`
	DiskTotal       int64          `json:"disk_total"`
	DiskUsed        int64          `json:"disk_used"`
	Load1           float64        `json:"load1"`
	Load5           float64        `json:"load5"`
	Load15          float64        `json:"load15"`
	Uptime          int64          `json:"uptime"`
	TCPCount        int            `json:"tcp_count"`
	Net             []NetInterface `json:"net"`
	NetUpload       int64          `json:"net_upload"`
	NetDown         int64          `json:"net_down"`
	Ping            map[string]int `json:"ping"`
	IP              string         `json:"ip"`
	Timestamp       int64          `json:"timestamp"`
	VnstatToday     int64          `json:"vnstat_today"`
	VnstatMonth     int64          `json:"vnstat_month"`
	VnstatTotal     int64          `json:"vnstat_total"`
	VnstatTodayUp   int64          `json:"vnstat_today_up"`
	VnstatTodayDown int64          `json:"vnstat_today_down"`
	VnstatMonthUp   int64          `json:"vnstat_month_up"`
	VnstatMonthDown int64          `json:"vnstat_month_down"`
	VnstatTotalUp   int64          `json:"vnstat_total_up"`
	VnstatTotalDown int64          `json:"vnstat_total_down"`
	TopCPU          []Process      `json:"top_cpu"`
	TopMem          []Process      `json:"top_mem"`
}

// ========== 网卡历史 ==========

var (
	lastNet   = make(map[string]map[string]int64) // name -> {rx, tx, time}
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

// ========== CPU ==========

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

// ========== 内存 ==========

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

// ========== 磁盘 ==========

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

// ========== 负载 ==========

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

// ========== Protocol ==========

func detectProtocol() string {
	has4, has6 := false, false
	ifaces, err := net.Interfaces()
	if err != nil {
		return "IPv4"
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ip, _, err := net.ParseCIDR(addr.String())
			if err != nil {
				continue
			}
			if ip.To4() != nil {
				has4 = true
			} else {
				has6 = true
			}
		}
	}
	if has4 && has6 {
		return "IPv4/IPv6"
	}
	if has6 {
		return "IPv6"
	}
	return "IPv4"
}

// ========== Uptime ==========

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

// ========== TCP 连接数 ==========

func readTCPCount() int {
	data, err := os.ReadFile("/proc/net/tcp")
	if err != nil {
		return 0
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) > 0 {
		return len(lines) - 2 // 减去 header 和空行
	}
	return 0
}

// ========== Ping ==========

func pingHost(host string) int {
	// 兼容不同 ping 实现；有些系统即使收到回包，也会把结果写到 stderr。
	commands := [][]string{
		{"-n", "-c", "1", "-W", "2", host},
		{"-n", "-c", "1", "-w", "3", host},
	}
	re := regexp.MustCompile(`time[=<]([0-9]+(?:\.[0-9]+)?)`)

	for _, args := range commands {
		cmd := exec.Command("ping", args...)
		out, _ := cmd.CombinedOutput()
		matches := re.FindSubmatch(out)
		if len(matches) < 2 {
			continue
		}
		ms, err := strconv.ParseFloat(string(matches[1]), 64)
		if err != nil {
			continue
		}
		if ms < 0 {
			continue
		}
		return int(ms)
	}
	return -1
}

func doPing() map[string]int {
	targets := []string{"112.64.212.1", "202.96.209.5", "211.136.112.50"}
	results := make(map[string]int)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, t := range targets {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			ms := pingHost(host)
			mu.Lock()
			results[host] = ms
			mu.Unlock()
		}(t)
	}
	wg.Wait()
	return results
}

// ========== vnstat ==========

func readVnstat() (today, todayUp, todayDown, month, monthUp, monthDown, total, totalUp, totalDown int64) {
	// 先取完整 JSON，避免 "d" 模式下 month/total 缺失导致数据为 0
	cmd := exec.Command("vnstat", "--json")
	out, err := cmd.Output()
	if err != nil {
		// 兼容老版本 vnstat 参数行为
		cmd = exec.Command("vnstat", "--json", "d")
		out, err = cmd.Output()
	}
	if err != nil {
		return
	}

	var vnstat struct {
		Interfaces []struct {
			Name    string `json:"name"`
			Traffic struct {
				Day []struct {
					ID   int `json:"id"`
					Date struct {
						Year  int `json:"year"`
						Month int `json:"month"`
						Day   int `json:"day"`
					} `json:"date"`
					Rx int64 `json:"rx"`
					Tx int64 `json:"tx"`
				} `json:"day"`
				Month []struct {
					ID   int `json:"id"`
					Date struct {
						Year  int `json:"year"`
						Month int `json:"month"`
					} `json:"date"`
					Rx int64 `json:"rx"`
					Tx int64 `json:"tx"`
				} `json:"month"`
				Total struct {
					Rx int64 `json:"rx"`
					Tx int64 `json:"tx"`
				} `json:"total"`
			} `json:"traffic"`
		} `json:"interfaces"`
	}

	if err := json.Unmarshal(out, &vnstat); err != nil {
		return
	}

	now := time.Now()
	for _, iface := range vnstat.Interfaces {
		var ifaceDayTotal int64
		var ifaceDayTotalUp int64
		var ifaceDayTotalDown int64
		var ifaceMonth int64
		var ifaceMonthUp int64
		var ifaceMonthDown int64

		// 今天
		for _, d := range iface.Traffic.Day {
			// 月兜底：有些环境 month 字段为空时，用 day 累计当前月
			if d.Date.Year == now.Year() && d.Date.Month == int(now.Month()) {
				ifaceDayTotalUp += d.Tx
				ifaceDayTotalDown += d.Rx
				ifaceDayTotal += d.Rx + d.Tx
			}
			if d.Date.Year == now.Year() && d.Date.Month == int(now.Month()) && d.Date.Day == now.Day() {
				todayUp += d.Tx
				todayDown += d.Rx
				today += d.Rx + d.Tx
			}
		}
		// 本月
		for _, m := range iface.Traffic.Month {
			if m.Date.Year == now.Year() && m.Date.Month == int(now.Month()) {
				ifaceMonthUp += m.Tx
				ifaceMonthDown += m.Rx
				ifaceMonth += m.Rx + m.Tx
			}
		}

		// total 在新装机器上可能暂时为 0，先取原值，后面再做兜底
		ifaceTotal := iface.Traffic.Total.Rx + iface.Traffic.Total.Tx
		ifaceTotalUp := iface.Traffic.Total.Tx
		ifaceTotalDown := iface.Traffic.Total.Rx

		// month 兜底（month 字段缺失或未累计时）
		if ifaceMonth == 0 && ifaceDayTotal > 0 {
			ifaceMonth = ifaceDayTotal
			ifaceMonthUp = ifaceDayTotalUp
			ifaceMonthDown = ifaceDayTotalDown
		}
		month += ifaceMonth
		monthUp += ifaceMonthUp
		monthDown += ifaceMonthDown

		// total 兜底：至少不小于当前月累计
		if ifaceTotal == 0 && ifaceDayTotal > 0 {
			ifaceTotal = ifaceDayTotal
			ifaceTotalUp = ifaceDayTotalUp
			ifaceTotalDown = ifaceDayTotalDown
		}

		total += ifaceTotal
		totalUp += ifaceTotalUp
		totalDown += ifaceTotalDown
	}
	return
}

// ========== 进程 Top ==========

var reProc = regexp.MustCompile(`^\s*(\d+)\s+(\S+)\s+(\S+)\s+(\d+)\s+(\d+)\s+.*`)

func readTopProcesses() (topCPU []Process, topMem []Process) {
	// 用 ps 获取进程信息
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

	// CPU Top 5
	sort.Slice(cpuList, func(i, j int) bool { return cpuList[i].CPU > cpuList[j].CPU })
	if len(cpuList) > 5 {
		cpuList = cpuList[:5]
	}
	topCPU = cpuList

	// Mem Top 5
	sort.Slice(memList, func(i, j int) bool { return memList[i].Memory > memList[j].Memory })
	if len(memList) > 5 {
		memList = memList[:5]
	}
	topMem = memList

	return
}

// ========== 客户端 IP ==========

func getClientIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "unknown"
	}
	defer conn.Close()
	addr := conn.LocalAddr().(*net.UDPAddr)
	return addr.IP.String()
}

// ========== 上报 ==========

func report(serverURL, name string) {
	clientIP := getClientIP()
	pingResults := doPing()
	cpuCores := runtime.NumCPU()

	for {
		start := time.Now()

		nets, netUp, netDown := readNetInterfaces()
		memPct, memTotal, memUsed, swapTotal, swapUsed := readMemory()
		diskPct, diskTotal, diskUsed := readDisk()
		l1, l5, l15 := readLoad()
		uptime := readUptime()
		tcpCount := readTCPCount()
		vToday, vTodayUp, vTodayDown, vMonth, vMonthUp, vMonthDown, vTotal, vTotalUp, vTotalDown := readVnstat()
		topCPU, topMem := readTopProcesses()
		cpu := readCPU()

		data := ServerData{
			Name:            name,
			CPU:             cpu,
			CPUCores:        cpuCores,
			Memory:          memPct,
			MemoryTotal:     memTotal,
			MemoryUsed:      memUsed,
			SwapTotal:       swapTotal,
			SwapUsed:        swapUsed,
			Disk:            diskPct,
			DiskTotal:       diskTotal,
			DiskUsed:        diskUsed,
			Load1:           l1,
			Load5:           l5,
			Load15:          l15,
			Uptime:          uptime,
			TCPCount:        tcpCount,
			Net:             nets,
			NetUpload:       netUp,
			NetDown:         netDown,
			Ping:            pingResults,
			IP:              clientIP,
			Timestamp:       time.Now().UnixMilli(),
			VnstatToday:     vToday,
			VnstatTodayUp:   vTodayUp,
			VnstatTodayDown: vTodayDown,
			VnstatMonth:     vMonth,
			VnstatMonthUp:   vMonthUp,
			VnstatMonthDown: vMonthDown,
			VnstatTotal:     vTotal,
			VnstatTotalUp:   vTotalUp,
			VnstatTotalDown: vTotalDown,
			Protocol:        detectProtocol(),
			TopCPU:          topCPU,
			TopMem:          topMem,
		}

		body, _ := json.Marshal(data)
		resp, err := http.Post(serverURL+"/api/report", "application/json", bytes.NewReader(body))
		if err != nil {
			log.Printf("❌ 上报失败: %v", err)
		} else {
			io.ReadAll(resp.Body)
			resp.Body.Close()
			elapsed := time.Since(start)
			log.Printf("✅ %s | CPU:%.1f%% MEM:%.1f%% NET:↑%s ↓%s | %dms",
				name, cpu, memPct, formatBytes(netUp), formatBytes(netDown), elapsed.Milliseconds())
		}

		// 每 30 秒重新 ping（太频繁没必要）
		if time.Now().Second() < 2 {
			go func() {
				pingResults = doPing()
			}()
		}

		time.Sleep(1 * time.Second)
	}
}

func formatBytes(b int64) string {
	if b < 1024 {
		return fmt.Sprintf("%d B/s", b)
	} else if b < 1024*1024 {
		return fmt.Sprintf("%.1f KB/s", float64(b)/1024)
	} else {
		return fmt.Sprintf("%.1f MB/s", float64(b)/(1024*1024))
	}
}

// ========== 主函数 ==========

func main() {
	serverURL := "http://localhost:9092"
	name, _ := os.Hostname()

	if len(os.Args) > 1 {
		serverURL = os.Args[1]
	}
	if !strings.HasPrefix(serverURL, "http://") && !strings.HasPrefix(serverURL, "https://") {
		serverURL = "http://" + serverURL
	}
	if len(os.Args) > 2 {
		name = os.Args[2]
	}

	log.Printf("🚀 ServerStatus Client 启动 → %s (%s)", serverURL, name)
	report(serverURL, name)
}
