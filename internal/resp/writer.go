package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// Writer RESP 协议写入器
type Writer struct {
	w *bufio.Writer
}

// NewWriter 创建一个新的 RESP 写入器
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriterSize(w, 4096)}
}

// Flush 刷新缓冲区
func (wr *Writer) Flush() error {
	return wr.w.Flush()
}

// WriteSimpleString 写入简单字符串：+OK\r\n
func (wr *Writer) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(wr.w, "+%s\r\n", s)
	return err
}

// WriteError 写入错误：-ERR message\r\n
func (wr *Writer) WriteError(msg string) error {
	_, err := fmt.Fprintf(wr.w, "-ERR %s\r\n", msg)
	return err
}

// WriteErrorRaw 写入原始错误（不加 ERR 前缀）
func (wr *Writer) WriteErrorRaw(msg string) error {
	_, err := fmt.Fprintf(wr.w, "-%s\r\n", msg)
	return err
}

// WriteInteger 写入整数：:42\r\n
func (wr *Writer) WriteInteger(n int64) error {
	_, err := fmt.Fprintf(wr.w, ":%d\r\n", n)
	return err
}

// WriteBulkString 写入批量字符串：$6\r\nfoobar\r\n
func (wr *Writer) WriteBulkString(s string) error {
	_, err := fmt.Fprintf(wr.w, "$%d\r\n%s\r\n", len(s), s)
	return err
}

// WriteNilBulk 写入 Nil 批量字符串：$-1\r\n
func (wr *Writer) WriteNilBulk() error {
	_, err := wr.w.WriteString("$-1\r\n")
	return err
}

// WriteArray 写入数组头：*n\r\n（之后需调用 n 次各元素的写入）
func (wr *Writer) WriteArrayHeader(n int) error {
	_, err := fmt.Fprintf(wr.w, "*%d\r\n", n)
	return err
}

// WriteNilArray 写入 Nil 数组：*-1\r\n
func (wr *Writer) WriteNilArray() error {
	_, err := wr.w.WriteString("*-1\r\n")
	return err
}

// WriteStringArray 写入字符串数组（所有元素作为 Bulk String）
func (wr *Writer) WriteStringArray(items []string) error {
	if err := wr.WriteArrayHeader(len(items)); err != nil {
		return err
	}
	for _, item := range items {
		if err := wr.WriteBulkString(item); err != nil {
			return err
		}
	}
	return nil
}

// WriteOK 写入 +OK\r\n
func (wr *Writer) WriteOK() error {
	return wr.WriteSimpleString("OK")
}

// WriteValue 写入一个 Value
func (wr *Writer) WriteValue(v *Value) error {
	switch v.Type {
	case '+':
		return wr.WriteSimpleString(v.Str)
	case '-':
		return wr.WriteErrorRaw(v.Str)
	case ':':
		return wr.WriteInteger(v.Integer)
	case '$':
		if v.IsNil {
			return wr.WriteNilBulk()
		}
		return wr.WriteBulkString(v.Str)
	case '*':
		if v.IsNil {
			return wr.WriteNilArray()
		}
		if err := wr.WriteArrayHeader(len(v.Array)); err != nil {
			return err
		}
		for _, elem := range v.Array {
			if err := wr.WriteValue(elem); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown value type: %c", v.Type)
	}
}

// ---- 帮助方法 ----

// IntToStr 将整数转为字符串（用于 score 等场景）
func IntToStr(n int64) string {
	return strconv.FormatInt(n, 10)
}
