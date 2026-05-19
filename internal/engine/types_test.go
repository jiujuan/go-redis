package engine

import "testing"

func TestValueTypeString(t *testing.T) {
	tests := []struct {
		name string
		typ  ValueType
		want string
	}{
		{name: "string", typ: TypeString, want: "string"},
		{name: "hash", typ: TypeHash, want: "hash"},
		{name: "list", typ: TypeList, want: "list"},
		{name: "set", typ: TypeSet, want: "set"},
		{name: "zset", typ: TypeZSet, want: "zset"},
		{name: "unknown", typ: ValueType(99), want: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.typ.String(); got != tt.want {
				t.Fatalf("ValueType(%d).String() = %q, want %q", tt.typ, got, tt.want)
			}
		})
	}
}

func TestFormatScore(t *testing.T) {
	tests := []struct {
		name  string
		score float64
		want  string
	}{
		{name: "integer", score: 12, want: "12"},
		{name: "negative integer", score: -7, want: "-7"},
		{name: "decimal", score: 1.25, want: "1.25"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatScore(tt.score); got != tt.want {
				t.Fatalf("formatScore(%v) = %q, want %q", tt.score, got, tt.want)
			}
		})
	}
}

func TestParseScore(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    float64
		wantErr bool
	}{
		{name: "integer", input: "2", want: 2},
		{name: "decimal", input: "2.5", want: 2.5},
		{name: "negative", input: "-3.5", want: -3.5},
		{name: "invalid", input: "bad", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseScore(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseScore(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestWrongTypeErr(t *testing.T) {
	if err := wrongTypeErr(); err != ErrWrongType {
		t.Fatalf("wrongTypeErr() = %v, want %v", err, ErrWrongType)
	}
}

func TestErrorValues(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "key not found", err: ErrKeyNotFound, want: "key not found"},
		{name: "wrong type", err: ErrWrongType, want: "WRONGTYPE operation against a key holding the wrong kind of value"},
		{name: "out of range", err: ErrOutOfRange, want: "index out of range"},
		{name: "empty list", err: ErrEmptyList, want: "list is empty"},
		{name: "member not found", err: ErrMemberNotFound, want: "member not found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Error() != tt.want {
				t.Fatalf("error string = %q, want %q", tt.err.Error(), tt.want)
			}
		})
	}
}
