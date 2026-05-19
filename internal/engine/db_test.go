package engine

import "testing"

func TestWithShardCountOptionValidation(t *testing.T) {
	tests := []struct {
		name  string
		count int
		want  int
	}{
		{name: "power of two", count: 8, want: 8},
		{name: "zero ignored", count: 0, want: defaultShardCount},
		{name: "negative ignored", count: -4, want: defaultShardCount},
		{name: "non power of two ignored", count: 6, want: defaultShardCount},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := NewGoRedis(WithShardCount(tt.count))
			if db.shardCount != tt.want {
				t.Fatalf("shardCount = %d, want %d", db.shardCount, tt.want)
			}
			if len(db.store.shards) != tt.want {
				t.Fatalf("backing shard count = %d, want %d", len(db.store.shards), tt.want)
			}
		})
	}
}

func TestParseInt64(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{name: "empty string", input: "", want: 0},
		{name: "positive", input: "42", want: 42},
		{name: "negative", input: "-9", want: -9},
		{name: "invalid", input: "abc", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseInt64(tt.input)
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
				t.Fatalf("parseInt64(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestFormatInt64(t *testing.T) {
	if got := formatInt64(-123); got != "-123" {
		t.Fatalf("formatInt64(-123) = %q, want %q", got, "-123")
	}
}

func TestErrorf(t *testing.T) {
	err := errorf("key %s failed: %d", "user:1", 2)
	if err == nil {
		t.Fatal("expected non-nil error")
	}
	if got := err.Error(); got != "key user:1 failed: 2" {
		t.Fatalf("errorf() = %q, want %q", got, "key user:1 failed: 2")
	}
}

func TestMatchPatternEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		str     string
		want    bool
	}{
		{name: "empty matches empty", pattern: "", str: "", want: true},
		{name: "empty does not match text", pattern: "", str: "x", want: false},
		{name: "question matches one byte", pattern: "?", str: "a", want: true},
		{name: "question does not match empty", pattern: "?", str: "", want: false},
		{name: "multiple stars", pattern: "a**d", str: "abcd", want: true},
		{name: "star suffix", pattern: "*tail", str: "longtail", want: true},
		{name: "mixed wildcards", pattern: "ab*?d", str: "abZZcd", want: true},
		{name: "mixed wildcards mismatch", pattern: "ab*?d", str: "abZZc", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchPattern(tt.pattern, tt.str); got != tt.want {
				t.Fatalf("matchPattern(%q, %q) = %v, want %v", tt.pattern, tt.str, got, tt.want)
			}
		})
	}
}

func TestDBKeysPatternQuestionMark(t *testing.T) {
	db := NewGoRedis()
	if err := db.Set("ab1", "1"); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("ab2", "2"); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("ab12", "12"); err != nil {
		t.Fatal(err)
	}

	keys := db.Keys("ab?")
	if len(keys) != 2 {
		t.Fatalf("len(Keys(\"ab?\")) = %d, want 2", len(keys))
	}
}
