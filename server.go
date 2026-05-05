package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

//go:embed index.html
var indexHTML string

// ========== 数据结构 ==========

type NetInterface struct {
	Name        string `json:"name"`
	Upload      int64  `json:"upload"`
	Download    int64  `json:"download"`
	UploadRate  int64  `json:"upload_rate"`
	DownloadRate int64 `json:"download_rate"`
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
	Name        string         `json:"name"`
	Protocol    string         `json:"protocol"`
	CPU         float64        `json:"cpu"`
	CPUCores    int            `json:"cpu_cores"`
	Memory      float64        `json:"memory"`
	MemoryTotal int64          `json:"memory_total"`
	MemoryUsed  int64          `json:"memory_used"`
	SwapTotal   int64          `json:"swap_total"`
	SwapUsed    int64          `json:"swap_used"`
	Disk        float64        `json:"disk"`
	DiskTotal   int64          `json:"disk_total"`
	DiskUsed    int64          `json:"disk_used"`
	Load1       float64        `json:"load1"`
	Load5       float64        `json:"load5"`
	Load15      float64        `json:"load15"`
	Uptime      int64          `json:"uptime"`
	TCPCount    int            `json:"tcp_count"`
	Net         []NetInterface `json:"net"`
	NetUpload   int64          `json:"net_upload"`
	NetDown     int64          `json:"net_down"`
	Ping        map[string]int `json:"ping"`
	IP          string         `json:"ip"`
	Timestamp   int64          `json:"timestamp"`
	// vnstat
	VnstatToday       int64 `json:"vnstat_today"`
	VnstatMonth       int64 `json:"vnstat_month"`
	VnstatTotal       int64 `json:"vnstat_total"`
	VnstatTodayUp     int64 `json:"vnstat_today_up"`
	VnstatTodayDown   int64 `json:"vnstat_today_down"`
	VnstatMonthUp     int64 `json:"vnstat_month_up"`
	VnstatMonthDown   int64 `json:"vnstat_month_down"`
	VnstatTotalUp     int64 `json:"vnstat_total_up"`
	VnstatTotalDown   int64 `json:"vnstat_total_down"`
	// 进程 Top
	TopCPU    []Process `json:"top_cpu"`
	TopMem    []Process `json:"top_mem"`
}

// ========== WebSocket Hub ==========

type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
}

type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	dropped    uint64
	mu         sync.RWMutex
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			count := len(h.clients)
			h.mu.Unlock()
			log.Printf("[WS] 客户端连接 (%d 个)", count)

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			count := len(h.clients)
			h.mu.Unlock()
			log.Printf("[WS] 客户端断开 (%d 个)", count)

		case message := <-h.broadcast:
			var stale []*Client
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					stale = append(stale, client)
				}
			}
			h.mu.RUnlock()
			if len(stale) == 0 {
				continue
			}
			h.mu.Lock()
			for _, client := range stale {
				if _, ok := h.clients[client]; ok {
					delete(h.clients, client)
					close(client.send)
				}
			}
			h.mu.Unlock()
		}
	}
}

func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *Hub) Broadcast(msg []byte) bool {
	select {
	case h.broadcast <- msg:
		return true
	default:
		atomic.AddUint64(&h.dropped, 1)
		return false
	}
}

func (h *Hub) DroppedCount() uint64 {
	return atomic.LoadUint64(&h.dropped)
}

// ========== Ring Buffer (历史数据) ==========

type RingBuffer struct {
	data   []*ServerData
	size   int
	pos    int
	count  int
	mu     sync.RWMutex
}

func newRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		data: make([]*ServerData, size),
		size: size,
	}
}

func (r *RingBuffer) Push(d *ServerData) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[r.pos] = d
	r.pos = (r.pos + 1) % r.size
	if r.count < r.size {
		r.count++
	}
}

func (r *RingBuffer) GetAll() []*ServerData {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.count == 0 {
		return nil
	}
	result := make([]*ServerData, r.count)
	start := (r.pos - r.count + r.size) % r.size
	for i := 0; i < r.count; i++ {
		result[i] = r.data[(start+i)%r.size]
	}
	return result
}

// ========== EMA 平滑 ==========

type EMASmooth struct {
	upload   float64
	download float64
	alpha    float64
	ready    bool
}

func newEMASmooth(alpha float64) *EMASmooth {
	return &EMASmooth{alpha: alpha}
}

func (e *EMASmooth) Update(up, down int64) (int64, int64) {
	if !e.ready {
		e.upload = float64(up)
		e.download = float64(down)
		e.ready = true
		return up, down
	}
	e.upload = e.alpha*float64(up) + (1-e.alpha)*e.upload
	e.download = e.alpha*float64(down) + (1-e.alpha)*e.download
	return int64(e.upload), int64(e.download)
}

// ========== 全局状态 ==========

var (
	dataStore   = make(map[string]*ServerData)
	dataStoreMu sync.RWMutex
	history     = newRingBuffer(3000) // 5 分钟 @ 100ms
	emaSmooth   = make(map[string]*EMASmooth)
	emaMu       sync.Mutex
	allowedOrigins = loadAllowedOrigins(os.Getenv("ALLOWED_ORIGINS"))
	upgrader       = websocket.Upgrader{
		CheckOrigin: checkOrigin,
	}
)

const clientTTL = 10 * time.Second
const maxReportBodyBytes = 1 << 20 // 1MB

func getEMA(name string) *EMASmooth {
	emaMu.Lock()
	defer emaMu.Unlock()
	if e, ok := emaSmooth[name]; ok {
		return e
	}
	e := newEMASmooth(0.3)
	emaSmooth[name] = e
	return e
}

// ========== HTTP Handlers ==========

func handleReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", 405)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxReportBodyBytes)
	var data ServerData
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&data); err != nil {
		http.Error(w, "Invalid JSON", 400)
		return
	}

	// 记录 client IP
	if data.IP == "" {
		data.IP = r.RemoteAddr
		if host, _, err := net.SplitHostPort(data.IP); err == nil {
			data.IP = host
		}
	}
	data.Timestamp = time.Now().UnixMilli()

	// EMA 平滑网速
	ema := getEMA(data.Name)
	data.NetUpload, data.NetDown = ema.Update(data.NetUpload, data.NetDown)

	// 同步平滑每个网卡
	for i := range data.Net {
		cardEma := getEMA(data.Name + ":" + data.Net[i].Name)
		data.Net[i].UploadRate, data.Net[i].DownloadRate = cardEma.Update(data.Net[i].UploadRate, data.Net[i].DownloadRate)
	}

	// 存储
	dataStoreMu.Lock()
	dataStore[data.Name] = &data
	dataStoreMu.Unlock()

	// 历史
	history.Push(&data)

	// 广播给所有 WebSocket 客户者
	msg, _ := json.Marshal(map[string]interface{}{
		"type": "update",
		"data": &data,
	})
	hub.Broadcast(msg)

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"ok":true}`)
}

func handleGetData(w http.ResponseWriter, r *http.Request) {
	dataStoreMu.RLock()
	all := make([]*ServerData, 0, len(dataStore))
	for _, v := range dataStore {
		all = append(all, v)
	}
	dataStoreMu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(all)
}

func handleGetHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(history.GetAll())
}

func handleWebSocket(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] 升级失败: %v", err)
		return
	}

	client := &Client{hub: hub, conn: conn, send: make(chan []byte, 64)}
	hub.register <- client

	// 发送历史数据
	historyData := history.GetAll()
	if len(historyData) > 0 {
		msg, _ := json.Marshal(map[string]interface{}{
			"type":    "history",
			"data":    historyData,
			"clients": hub.ClientCount(),
		})
		select {
		case client.send <- msg:
		default:
		}
	}

	// 发送当前状态
	dataStoreMu.RLock()
	all := make([]*ServerData, 0, len(dataStore))
	for _, v := range dataStore {
		all = append(all, v)
	}
	dataStoreMu.RUnlock()
	if len(all) > 0 {
		msg, _ := json.Marshal(map[string]interface{}{
			"type":    "snapshot",
			"data":    all,
			"clients": hub.ClientCount(),
		})
		select {
		case client.send <- msg:
		default:
		}
	}

	go clientWritePump(client)
	go clientReadPump(client)
}

func clientReadPump(c *Client) {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(512)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func clientWritePump(c *Client) {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)
			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// ========== 静态文件 ==========

func handleIndex(w http.ResponseWriter, r *http.Request) {
	html := []byte(indexHTML)

	// 服务端注入当前数据，页面加载就有内容
	dataStoreMu.RLock()
	all := make([]*ServerData, 0, len(dataStore))
	for _, v := range dataStore {
		all = append(all, v)
	}
	dataStoreMu.RUnlock()

	jsonData, _ := json.Marshal(all)
	pos := headPos(html)
	inject := string(html[:pos]) + fmt.Sprintf("<script>window.__INIT_DATA__=%s;</script>", string(jsonData)) + string(html[pos:])

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.Write([]byte(inject))
}

func headPos(html []byte) int {
	// 在 </head> 前注入
	idx := bytes.Index(html, []byte("</head>"))
	if idx < 0 {
		return 0
	}
	return idx
}

func snapshotData() []*ServerData {
	dataStoreMu.RLock()
	defer dataStoreMu.RUnlock()
	all := make([]*ServerData, 0, len(dataStore))
	for _, v := range dataStore {
		all = append(all, v)
	}
	return all
}

func loadAllowedOrigins(raw string) map[string]struct{} {
	out := make(map[string]struct{})
	if strings.TrimSpace(raw) == "" {
		return out
	}
	for _, item := range strings.Split(raw, ",") {
		v := strings.TrimSpace(item)
		if v == "" {
			continue
		}
		out[v] = struct{}{}
	}
	return out
}

func checkOrigin(r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	// 非浏览器客户端通常不会带 Origin。
	if origin == "" {
		return true
	}
	if len(allowedOrigins) == 0 {
		// 未配置白名单时，仅允许同主机来源。
		return strings.HasSuffix(origin, "://"+r.Host)
	}
	_, ok := allowedOrigins[origin]
	return ok
}

func pruneStaleClients(now time.Time) {
	expireBefore := now.Add(-clientTTL).UnixMilli()
	removed := false

	dataStoreMu.Lock()
	for name, v := range dataStore {
		if v == nil || v.Timestamp < expireBefore {
			delete(dataStore, name)
			removed = true
		}
	}
	remaining := len(dataStore)
	dataStoreMu.Unlock()

	if removed {
		log.Printf("[PRUNE] 清理离线节点，剩余 %d 台", remaining)
	}
}

// ========== 主函数 ==========

var hub *Hub

func main() {
	port := "9092"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	hub = newHub()
	go hub.run()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 每 1 秒广播一次当前状态（防止长时间无上报时前端卡住）
	tickerDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		defer close(tickerDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pruneStaleClients(time.Now())

				msg, _ := json.Marshal(map[string]interface{}{
					"type":    "tick",
					"data":    snapshotData(),
					"clients": hub.ClientCount(),
				})
				hub.Broadcast(msg)
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)
	mux.HandleFunc("/api/report", handleReport)
	mux.HandleFunc("/api/data", handleGetData)
	mux.HandleFunc("/api/history", handleGetHistory)
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handleWebSocket(hub, w, r)
	})

	addr := "0.0.0.0:" + port
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("🚀 ServerStatus 启动 → http://%s", addr)
	log.Printf("📡 WebSocket: ws://%s/ws", addr)
	if len(allowedOrigins) == 0 {
		log.Printf("⚠️  ALLOWED_ORIGINS 未配置，WS 仅允许同主机 Origin")
	} else {
		log.Printf("🔐 WS Origin 白名单已启用 (%d 条)", len(allowedOrigins))
	}
	go func() {
		<-ctx.Done()
		log.Printf("🛑 收到退出信号，开始优雅停机")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("⚠️  优雅停机失败: %v", err)
		}
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("启动失败: %v", err)
	}
	<-tickerDone
	log.Printf("✅ 服务已退出，广播丢弃总数: %d", hub.DroppedCount())
}
