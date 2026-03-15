// Package resp 实现 Redis 序列化协议（RESP）的编解码。
//
// RESP 数据类型：
//   - Simple String: "+OK\r\n"
//   - Error:         "-ERR message\r\n"
//   - Integer:       ":42\r\n"
//   - Bulk String:   "$6\r\nfoobar\r\n"  ($-1\r\n 表示 nil)
//   - Array:         "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"  (*-1\r\n 表示 nil)
package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// Value 表示一个 RESP 值
type Value struct {
	Type    byte   // '+' '-' ':' '$' '*'
	Str     string // Simple String / Error / Bulk String
	Integer int64  // Integer
	Array   []*Value
	IsNil   bool // Nil Bulk String / Nil Array
}

// Reader RESP 协议读取器
type Reader struct {
	r *bufio.Reader
}

// NewReader 创建一个新的 RESP 读取器
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, 4096)}
}

// Read 从连接中读取一个完整的 RESP 值
func (rd *Reader) Read() (*Value, error) {
	line, err := rd.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("empty line")
	}

	switch line[0] {
	case '+':
		return &Value{Type: '+', Str: line[1:]}, nil
	case '-':
		return &Value{Type: '-', Str: line[1:]}, nil
	case ':':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse integer: %w", err)
		}
		return &Value{Type: ':', Integer: n}, nil
	case '$':
		return rd.readBulkString(line[1:])
	case '*':
		return rd.readArray(line[1:])
	default:
		// 内联命令兼容（如 telnet 直接输入 "PING\r\n"）
		return rd.parseInline(string(line))
	}
}

func (rd *Reader) readLine() (string, error) {
	line, err := rd.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	// 去除末尾 \r\n
	if len(line) >= 2 && line[len(line)-2] == '\r' {
		return line[:len(line)-2], nil
	}
	return line[:len(line)-1], nil
}

func (rd *Reader) readBulkString(lenStr string) (*Value, error) {
	n, err := strconv.Atoi(lenStr)
	if err != nil {
		return nil, fmt.Errorf("parse bulk string length: %w", err)
	}
	if n < 0 {
		return &Value{Type: '$', IsNil: true}, nil
	}

	buf := make([]byte, n+2) // +2 for \r\n
	_, err = io.ReadFull(rd.r, buf)
	if err != nil {
		return nil, fmt.Errorf("read bulk string: %w", err)
	}
	return &Value{Type: '$', Str: string(buf[:n])}, nil
}

func (rd *Reader) readArray(lenStr string) (*Value, error) {
	n, err := strconv.Atoi(lenStr)
	if err != nil {
		return nil, fmt.Errorf("parse array length: %w", err)
	}
	if n < 0 {
		return &Value{Type: '*', IsNil: true}, nil
	}

	arr := make([]*Value, n)
	for i := 0; i < n; i++ {
		v, err := rd.Read()
		if err != nil {
			return nil, fmt.Errorf("read array element %d: %w", i, err)
		}
		arr[i] = v
	}
	return &Value{Type: '*', Array: arr}, nil
}

// parseInline 解析内联命令（如 "PING" 或 "SET key value"）
func (rd *Reader) parseInline(line string) (*Value, error) {
	parts := splitInline(line)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty inline command")
	}
	arr := make([]*Value, len(parts))
	for i, p := range parts {
		arr[i] = &Value{Type: '$', Str: p}
	}
	return &Value{Type: '*', Array: arr}, nil
}

// splitInline 简单按空格分割，支持单双引号
func splitInline(line string) []string {
	var parts []string
	var current []byte
	inQuote := byte(0)

	for i := 0; i < len(line); i++ {
		c := line[i]
		if inQuote != 0 {
			if c == inQuote {
				inQuote = 0
			} else {
				current = append(current, c)
			}
		} else if c == '\'' || c == '"' {
			inQuote = c
		} else if c == ' ' || c == '\t' {
			if len(current) > 0 {
				parts = append(parts, string(current))
				current = current[:0]
			}
		} else {
			current = append(current, c)
		}
	}
	if len(current) > 0 {
		parts = append(parts, string(current))
	}
	return parts
}

// ToArgs 将 RESP Array 中的 Bulk String 转换为字符串切片
func (v *Value) ToArgs() ([]string, error) {
	if v.Type != '*' {
		return nil, fmt.Errorf("expected array, got %c", v.Type)
	}
	args := make([]string, len(v.Array))
	for i, item := range v.Array {
		if item.Type != '$' {
			return nil, fmt.Errorf("expected bulk string at index %d, got %c", i, item.Type)
		}
		args[i] = item.Str
	}
	return args, nil
}
