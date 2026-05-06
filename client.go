package main

import (
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

// 平台相关采集实现见 client_linux.go、client_darwin.go

// ========== Ping ==========

func pingHost(host string) int {
	re := regexp.MustCompile(`time[=<]([0-9]+(?:\.[0-9]+)?)`)

	var variants [][]string
	switch runtime.GOOS {
	case "darwin":
		variants = [][]string{
			{"-c", "1", "-W", "2000", host},
			{"-c", "1", "-t", "3", host},
		}
	default:
		variants = [][]string{
			{"-n", "-c", "1", "-W", "2", host},
			{"-n", "-c", "1", "-w", "3", host},
		}
	}

	for _, args := range variants {
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

func vnstatExecutable() string {
	if runtime.GOOS == "darwin" {
		for _, p := range []string{
			"/opt/homebrew/bin/vnstat",
			"/usr/local/bin/vnstat",
		} {
			if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
				return p
			}
		}
	}
	if p, err := exec.LookPath("vnstat"); err == nil {
		return p
	}
	return ""
}

// extractJSONObject 去掉 vnstat 可能在 JSON 前的提示行，避免 Unmarshal 失败。
func extractJSONObject(out []byte) []byte {
	out = bytes.TrimSpace(out)
	if i := bytes.IndexByte(out, '{'); i >= 0 {
		return out[i:]
	}
	return out
}

func readVnstat() (today, todayUp, todayDown, month, monthUp, monthDown, total, totalUp, totalDown int64) {
	bin := vnstatExecutable()
	if bin == "" {
		return
	}

	var out []byte
	for _, args := range [][]string{{"--json"}, {"--json", "d"}} {
		cmd := exec.Command(bin, args...)
		raw, _ := cmd.CombinedOutput()
		raw = extractJSONObject(raw)
		if len(raw) > 0 && raw[0] == '{' {
			out = raw
			break
		}
	}
	if len(out) == 0 {
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

		for _, d := range iface.Traffic.Day {
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
		for _, m := range iface.Traffic.Month {
			if m.Date.Year == now.Year() && m.Date.Month == int(now.Month()) {
				ifaceMonthUp += m.Tx
				ifaceMonthDown += m.Rx
				ifaceMonth += m.Rx + m.Tx
			}
		}

		ifaceTotal := iface.Traffic.Total.Rx + iface.Traffic.Total.Tx
		ifaceTotalUp := iface.Traffic.Total.Tx
		ifaceTotalDown := iface.Traffic.Total.Rx

		if ifaceMonth == 0 && ifaceDayTotal > 0 {
			ifaceMonth = ifaceDayTotal
			ifaceMonthUp = ifaceDayTotalUp
			ifaceMonthDown = ifaceDayTotalDown
		}
		month += ifaceMonth
		monthUp += ifaceMonthUp
		monthDown += ifaceMonthDown

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

const reportPeriod = time.Second

func report(serverURL, name string) {
	reportWithTicker(serverURL, name)
}

func reportWithTicker(serverURL, name string) {
	clientIP := getClientIP()
	pingResults := doPing()
	cpuCores := runtime.NumCPU()

	ticker := time.NewTicker(reportPeriod)
	defer ticker.Stop()

	for {
		tickStart := time.Now()

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
			elapsed := time.Since(tickStart)
			log.Printf("✅ %s | CPU:%.1f%% MEM:%.1f%% NET:↑%s ↓%s | %dms",
				name, cpu, memPct, formatBytes(netUp), formatBytes(netDown), elapsed.Milliseconds())
		}

		if time.Now().Second() < 2 {
			go func() {
				pingResults = doPing()
			}()
		}

		<-ticker.C
	}
}

func reportWithUntil(serverURL, name string) {
	clientIP := getClientIP()
	pingResults := doPing()
	cpuCores := runtime.NumCPU()

	for {
		tickStart := time.Now()

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
			elapsed := time.Since(tickStart)
			log.Printf("✅ %s | CPU:%.1f%% MEM:%.1f%% NET:↑%s ↓%s | %dms",
				name, cpu, memPct, formatBytes(netUp), formatBytes(netDown), elapsed.Milliseconds())
		}

		if time.Now().Second() < 2 {
			go func() {
				pingResults = doPing()
			}()
		}

		time.Sleep(time.Until(tickStart.Add(reportPeriod)))
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
