package resp_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/jiujuan/go-redis/internal/resp"
)

// helper: write bytes into a Reader
func newReader(s string) *resp.Reader {
	return resp.NewReader(strings.NewReader(s))
}

// helper: capture Writer output
func newWriter() (*resp.Writer, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return resp.NewWriter(buf), buf
}

// ─────────────────────────────────────────────
//  Reader tests
// ─────────────────────────────────────────────

func TestReader_SimpleString(t *testing.T) {
	r := newReader("+OK\r\n")
	v, err := r.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if v.Type != '+' || v.Str != "OK" {
		t.Errorf("SimpleString: got type=%c str=%q", v.Type, v.Str)
	}
}

func TestReader_SimpleString_Empty(t *testing.T) {
	r := newReader("+\r\n")
	v, err := r.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if v.Str != "" {
		t.Errorf("empty simple string: got %q", v.Str)
	}
}

func TestReader_Error(t *testing.T) {
	r := newReader("-ERR something went wrong\r\n")
	v, err := r.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if v.Type != '-' || v.Str != "ERR something went wrong" {
		t.Errorf("Error: got type=%c str=%q", v.Type, v.Str)
	}
}

func TestReader_Integer(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  int64
	}{
		{":0\r\n", 0},
		{":42\r\n", 42},
		{":-7\r\n", -7},
		{":9999999999\r\n", 9999999999},
	} {
		r := newReader(tc.input)
		v, err := r.Read()
		if err != nil {
			t.Fatalf("input %q Read: %v", tc.input, err)
		}
		if v.Type != ':' || v.Integer != tc.want {
			t.Errorf("Integer %q: got %d, want %d", tc.input, v.Integer, tc.want)
		}
	}
}

func TestReader_BulkString(t *testing.T) {
	r := newReader("$6\r\nfoobar\r\n")
	v, err := r.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if v.Type != '$' || v.Str != "foobar" || v.IsNil {
		t.Errorf("BulkString: type=%c str=%q isNil=%v", v.Type, v.Str, v.IsNil)
	}
}

func TestReader_BulkString_Empty(t *testing.T) {
	r := newReader("$0\r\n\r\n")
	v, err := r.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if v.Str != "" || v.IsNil {
		t.Errorf("empty bulk string: got str=%q isNil=%v", v.Str, v.IsNil)
	}
}

func TestReader_NilBulkString(t *testing.T) {
	r := newReader("$-1\r\n")
	v, err := r.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !v.IsNil {
		t.Error("nil bulk string should have IsNil=true")
	}
}

func TestReader_Array(t *testing.T) {
	r := newReader("*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n")
	v, err := r.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if v.Type != '*' || len(v.Array) != 3 {
		t.Fatalf("Array: type=%c len=%d", v.Type, len(v.Array))
	}
	if v.Array[0].Str != "foo" || v.Array[1].Str != "bar" || v.Array[2].Str != "baz" {
		t.Errorf("Array contents: %v", v.Array)
	}
}

func TestReader_Array_Empty(t *testing.T) {
	r := newReader("*0\r\n")
	v, err := r.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if v.Type != '*' || len(v.Array) != 0 {
		t.Errorf("empty array: type=%c len=%d", v.Type, len(v.Array))
	}
}

func TestReader_NilArray(t *testing.T) {
	r := newReader("*-1\r\n")
	v, err := r.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !v.IsNil {
		t.Error("nil array should have IsNil=true")
	}
}

func TestReader_NestedArray(t *testing.T) {
	// *2\r\n *2\r\n :1\r\n :2\r\n $5\r\nhello\r\n
	raw := "*2\r\n*2\r\n:1\r\n:2\r\n$5\r\nhello\r\n"
	r := newReader(raw)
	v, err := r.Read()
	if err != nil {
		t.Fatalf("Read nested: %v", err)
	}
	if len(v.Array) != 2 {
		t.Fatalf("outer array len=%d", len(v.Array))
	}
	inner := v.Array[0]
	if inner.Type != '*' || len(inner.Array) != 2 {
		t.Errorf("inner array: type=%c len=%d", inner.Type, len(inner.Array))
	}
}

func TestReader_Inline_PING(t *testing.T) {
	r := newReader("PING\r\n")
	v, err := r.Read()
	if err != nil {
		t.Fatalf("inline PING: %v", err)
	}
	if v.Type != '*' || len(v.Array) != 1 || v.Array[0].Str != "PING" {
		t.Errorf("inline PING: got %+v", v)
	}
}

func TestReader_Inline_WithArgs(t *testing.T) {
	r := newReader("SET key value\r\n")
	v, err := r.Read()
	if err != nil {
		t.Fatalf("inline SET: %v", err)
	}
	args, err := v.ToArgs()
	if err != nil {
		t.Fatalf("ToArgs: %v", err)
	}
	if len(args) != 3 || args[0] != "SET" || args[1] != "key" || args[2] != "value" {
		t.Errorf("inline args: %v", args)
	}
}

func TestReader_Inline_Quoted(t *testing.T) {
	r := newReader("SET key \"hello world\"\r\n")
	v, err := r.Read()
	if err != nil {
		t.Fatalf("Read quoted: %v", err)
	}
	args, _ := v.ToArgs()
	if len(args) != 3 || args[2] != "hello world" {
		t.Errorf("quoted inline: args=%v", args)
	}
}

func TestReader_MultipleMessages(t *testing.T) {
	r := newReader("+OK\r\n:42\r\n$5\r\nhello\r\n")
	types := []byte{'+', ':', '$'}
	for _, want := range types {
		v, err := r.Read()
		if err != nil {
			t.Fatalf("sequential read (want %c): %v", want, err)
		}
		if v.Type != want {
			t.Errorf("sequential type: got %c, want %c", v.Type, want)
		}
	}
}

func TestReader_ToArgs_NonArray(t *testing.T) {
	v := &resp.Value{Type: '+', Str: "OK"}
	_, err := v.ToArgs()
	if err == nil {
		t.Error("ToArgs on non-array should return error")
	}
}

func TestReader_ToArgs_MixedTypes(t *testing.T) {
	v := &resp.Value{
		Type: '*',
		Array: []*resp.Value{
			{Type: ':', Integer: 1}, // not a bulk string
		},
	}
	_, err := v.ToArgs()
	if err == nil {
		t.Error("ToArgs with non-bulk-string element should error")
	}
}

func TestReader_EOF(t *testing.T) {
	r := newReader("")
	_, err := r.Read()
	if err == nil {
		t.Error("reading from empty reader should return error")
	}
}

// ─────────────────────────────────────────────
//  Writer tests
// ─────────────────────────────────────────────

func TestWriter_WriteSimpleString(t *testing.T) {
	w, buf := newWriter()
	w.WriteSimpleString("OK")
	w.Flush()
	if buf.String() != "+OK\r\n" {
		t.Errorf("SimpleString: got %q", buf.String())
	}
}

func TestWriter_WriteOK(t *testing.T) {
	w, buf := newWriter()
	w.WriteOK()
	w.Flush()
	if buf.String() != "+OK\r\n" {
		t.Errorf("WriteOK: got %q", buf.String())
	}
}

func TestWriter_WriteError(t *testing.T) {
	w, buf := newWriter()
	w.WriteError("wrong type")
	w.Flush()
	if buf.String() != "-ERR wrong type\r\n" {
		t.Errorf("WriteError: got %q", buf.String())
	}
}

func TestWriter_WriteErrorRaw(t *testing.T) {
	w, buf := newWriter()
	w.WriteErrorRaw("WRONGTYPE custom")
	w.Flush()
	if buf.String() != "-WRONGTYPE custom\r\n" {
		t.Errorf("WriteErrorRaw: got %q", buf.String())
	}
}

func TestWriter_WriteInteger(t *testing.T) {
	cases := []struct {
		n    int64
		want string
	}{
		{0, ":0\r\n"},
		{42, ":42\r\n"},
		{-7, ":-7\r\n"},
	}
	for _, c := range cases {
		w, buf := newWriter()
		w.WriteInteger(c.n)
		w.Flush()
		if buf.String() != c.want {
			t.Errorf("WriteInteger(%d): got %q, want %q", c.n, buf.String(), c.want)
		}
	}
}

func TestWriter_WriteBulkString(t *testing.T) {
	w, buf := newWriter()
	w.WriteBulkString("foobar")
	w.Flush()
	if buf.String() != "$6\r\nfoobar\r\n" {
		t.Errorf("WriteBulkString: got %q", buf.String())
	}
}

func TestWriter_WriteBulkString_Empty(t *testing.T) {
	w, buf := newWriter()
	w.WriteBulkString("")
	w.Flush()
	if buf.String() != "$0\r\n\r\n" {
		t.Errorf("WriteBulkString empty: got %q", buf.String())
	}
}

func TestWriter_WriteNilBulk(t *testing.T) {
	w, buf := newWriter()
	w.WriteNilBulk()
	w.Flush()
	if buf.String() != "$-1\r\n" {
		t.Errorf("WriteNilBulk: got %q", buf.String())
	}
}

func TestWriter_WriteArrayHeader(t *testing.T) {
	w, buf := newWriter()
	w.WriteArrayHeader(3)
	w.Flush()
	if buf.String() != "*3\r\n" {
		t.Errorf("WriteArrayHeader: got %q", buf.String())
	}
}

func TestWriter_WriteNilArray(t *testing.T) {
	w, buf := newWriter()
	w.WriteNilArray()
	w.Flush()
	if buf.String() != "*-1\r\n" {
		t.Errorf("WriteNilArray: got %q", buf.String())
	}
}

func TestWriter_WriteStringArray(t *testing.T) {
	w, buf := newWriter()
	w.WriteStringArray([]string{"a", "b", "c"})
	w.Flush()
	want := "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
	if buf.String() != want {
		t.Errorf("WriteStringArray: got %q, want %q", buf.String(), want)
	}
}

func TestWriter_WriteStringArray_Empty(t *testing.T) {
	w, buf := newWriter()
	w.WriteStringArray([]string{})
	w.Flush()
	if buf.String() != "*0\r\n" {
		t.Errorf("WriteStringArray empty: got %q", buf.String())
	}
}

func TestWriter_WriteValue_SimpleString(t *testing.T) {
	w, buf := newWriter()
	w.WriteValue(&resp.Value{Type: '+', Str: "PONG"})
	w.Flush()
	if buf.String() != "+PONG\r\n" {
		t.Errorf("WriteValue SimpleString: got %q", buf.String())
	}
}

func TestWriter_WriteValue_Integer(t *testing.T) {
	w, buf := newWriter()
	w.WriteValue(&resp.Value{Type: ':', Integer: 99})
	w.Flush()
	if buf.String() != ":99\r\n" {
		t.Errorf("WriteValue Integer: got %q", buf.String())
	}
}

func TestWriter_WriteValue_NilBulk(t *testing.T) {
	w, buf := newWriter()
	w.WriteValue(&resp.Value{Type: '$', IsNil: true})
	w.Flush()
	if buf.String() != "$-1\r\n" {
		t.Errorf("WriteValue NilBulk: got %q", buf.String())
	}
}

func TestWriter_WriteValue_Array(t *testing.T) {
	w, buf := newWriter()
	v := &resp.Value{
		Type: '*',
		Array: []*resp.Value{
			{Type: '$', Str: "foo"},
			{Type: ':', Integer: 3},
		},
	}
	w.WriteValue(v)
	w.Flush()
	want := "*2\r\n$3\r\nfoo\r\n:3\r\n"
	if buf.String() != want {
		t.Errorf("WriteValue Array: got %q, want %q", buf.String(), want)
	}
}

func TestWriter_WriteValue_NilArray(t *testing.T) {
	w, buf := newWriter()
	w.WriteValue(&resp.Value{Type: '*', IsNil: true})
	w.Flush()
	if buf.String() != "*-1\r\n" {
		t.Errorf("WriteValue NilArray: got %q", buf.String())
	}
}

func TestWriter_WriteValue_UnknownType(t *testing.T) {
	w, _ := newWriter()
	err := w.WriteValue(&resp.Value{Type: 'X'})
	if err == nil {
		t.Error("WriteValue with unknown type should return error")
	}
}

// ─────────────────────────────────────────────
//  Round-trip: write then read back
// ─────────────────────────────────────────────

func TestRoundTrip_BulkString(t *testing.T) {
	w, buf := newWriter()
	w.WriteBulkString("hello世界")
	w.Flush()

	r := resp.NewReader(buf)
	v, err := r.Read()
	if err != nil {
		t.Fatalf("round-trip read: %v", err)
	}
	if v.Str != "hello世界" {
		t.Errorf("round-trip: got %q", v.Str)
	}
}

func TestRoundTrip_Array(t *testing.T) {
	w, buf := newWriter()
	w.WriteStringArray([]string{"SET", "mykey", "myvalue"})
	w.Flush()

	r := resp.NewReader(buf)
	v, err := r.Read()
	if err != nil {
		t.Fatalf("round-trip array read: %v", err)
	}
	args, err := v.ToArgs()
	if err != nil {
		t.Fatalf("ToArgs: %v", err)
	}
	if len(args) != 3 || args[0] != "SET" || args[1] != "mykey" || args[2] != "myvalue" {
		t.Errorf("round-trip array args: %v", args)
	}
}

func TestRoundTrip_Integer(t *testing.T) {
	w, buf := newWriter()
	w.WriteInteger(-12345)
	w.Flush()

	r := resp.NewReader(buf)
	v, err := r.Read()
	if err != nil {
		t.Fatalf("round-trip int: %v", err)
	}
	if v.Integer != -12345 {
		t.Errorf("round-trip int: got %d", v.Integer)
	}
}

func TestIntToStr(t *testing.T) {
	if resp.IntToStr(0) != "0" {
		t.Error("IntToStr(0)")
	}
	if resp.IntToStr(-42) != "-42" {
		t.Error("IntToStr(-42)")
	}
	if resp.IntToStr(1000000) != "1000000" {
		t.Error("IntToStr(1000000)")
	}
}
