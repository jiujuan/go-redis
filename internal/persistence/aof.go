// Package persistence 实现 go-redis 的持久化机制（v0.2）。
package persistence

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// AOF 追加写日志（Append Only File）
// 每条写命令以 RESP 格式追加到文件，重启时重放以恢复数据。
type AOF struct {
	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	filename string
	syncMode string // always | everysec | no
	ticker   *time.Ticker
	quit     chan struct{}
	wg       sync.WaitGroup
}

// NewAOF 创建 AOF 实例并打开文件
func NewAOF(filename, syncMode string) (*AOF, error) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open aof file: %w", err)
	}

	aof := &AOF{
		file:     f,
		writer:   bufio.NewWriterSize(f, 64*1024),
		filename: filename,
		syncMode: syncMode,
		quit:     make(chan struct{}),
	}

	if syncMode == "everysec" {
		aof.ticker = time.NewTicker(time.Second)
		aof.wg.Add(1)
		go aof.syncLoop()
	}

	return aof, nil
}

// Write 将命令写入 AOF 缓冲区
func (a *AOF) Write(args []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// 以 RESP Array 格式写入
	if _, err := fmt.Fprintf(a.writer, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(a.writer, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			return err
		}
	}

	if a.syncMode == "always" {
		return a.syncLocked()
	}
	return nil
}

func (a *AOF) syncLocked() error {
	if err := a.writer.Flush(); err != nil {
		return err
	}
	return a.file.Sync()
}

func (a *AOF) syncLoop() {
	defer a.wg.Done()
	for {
		select {
		case <-a.ticker.C:
			a.mu.Lock()
			a.syncLocked()
			a.mu.Unlock()
		case <-a.quit:
			return
		}
	}
}

// Close 关闭 AOF，确保数据落盘
func (a *AOF) Close() error {
	close(a.quit)
	if a.ticker != nil {
		a.ticker.Stop()
	}
	a.wg.Wait()

	a.mu.Lock()
	defer a.mu.Unlock()
	a.writer.Flush()
	a.file.Sync()
	return a.file.Close()
}

// Replay 重放 AOF 文件中的命令，调用 execFn 执行每条命令
// execFn(args []string) error
func Replay(filename string, execFn func([]string) error) error {
	f, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 文件不存在，跳过
		}
		return fmt.Errorf("open aof: %w", err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	lineNo := 0
	total := 0

	for {
		args, err := readCommand(reader, &lineNo)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("[aof] replay error at line %d: %v", lineNo, err)
			break
		}
		if len(args) == 0 {
			continue
		}
		if err := execFn(args); err != nil {
			log.Printf("[aof] exec error: %v (cmd: %s)", err, strings.Join(args, " "))
		}
		total++
	}

	log.Printf("[aof] replayed %d commands from %s", total, filename)
	return nil
}

// readCommand 从 bufio.Reader 读取一条 RESP 命令
func readCommand(r *bufio.Reader, lineNo *int) ([]string, error) {
	// 读 *N\r\n
	line, err := readLine(r, lineNo)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("expected '*', got %q", line)
	}

	var count int
	if _, err := fmt.Sscanf(line[1:], "%d", &count); err != nil {
		return nil, fmt.Errorf("parse count: %w", err)
	}

	args := make([]string, count)
	for i := 0; i < count; i++ {
		// 读 $N\r\n
		lenLine, err := readLine(r, lineNo)
		if err != nil {
			return nil, err
		}
		if len(lenLine) == 0 || lenLine[0] != '$' {
			return nil, fmt.Errorf("expected '$', got %q", lenLine)
		}
		var n int
		if _, err := fmt.Sscanf(lenLine[1:], "%d", &n); err != nil {
			return nil, fmt.Errorf("parse bulk len: %w", err)
		}

		// 读内容 + \r\n
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("read bulk: %w", err)
		}
		*lineNo++
		args[i] = string(buf[:n])
	}
	return args, nil
}

func readLine(r *bufio.Reader, lineNo *int) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	*lineNo++
	if len(line) >= 2 && line[len(line)-2] == '\r' {
		return line[:len(line)-2], nil
	}
	return line[:len(line)-1], nil
}
