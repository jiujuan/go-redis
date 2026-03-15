package config_test

import (
	"os"
	"testing"

	"github.com/jiujuan/go-redis/config"
)

func TestDefaultConfig(t *testing.T) {
	cfg := config.DefaultConfig()

	if cfg.Host != "0.0.0.0" {
		t.Errorf("Host: got %q, want %q", cfg.Host, "0.0.0.0")
	}
	if cfg.Port != 6379 {
		t.Errorf("Port: got %d, want 6379", cfg.Port)
	}
	if cfg.ShardCount != 256 {
		t.Errorf("ShardCount: got %d, want 256", cfg.ShardCount)
	}
	if cfg.AOFEnabled {
		t.Error("AOFEnabled should default to false")
	}
	if cfg.RDBEnabled {
		t.Error("RDBEnabled should default to false")
	}
	if cfg.AOFFilename != "appendonly.aof" {
		t.Errorf("AOFFilename: got %q, want %q", cfg.AOFFilename, "appendonly.aof")
	}
	if cfg.AOFSync != "everysec" {
		t.Errorf("AOFSync: got %q, want everysec", cfg.AOFSync)
	}
	if cfg.RDBFilename != "dump.rdb" {
		t.Errorf("RDBFilename: got %q, want dump.rdb", cfg.RDBFilename)
	}
	if cfg.MaxConnections != 10000 {
		t.Errorf("MaxConnections: got %d, want 10000", cfg.MaxConnections)
	}
	if cfg.VirtualReplicas != 150 {
		t.Errorf("VirtualReplicas: got %d, want 150", cfg.VirtualReplicas)
	}
}

func TestAddr(t *testing.T) {
	tests := []struct {
		host string
		port int
		want string
	}{
		{"0.0.0.0", 6379, "0.0.0.0:6379"},
		{"127.0.0.1", 6380, "127.0.0.1:6380"},
		{"localhost", 9999, "localhost:9999"},
	}
	for _, tt := range tests {
		cfg := &config.Config{Host: tt.host, Port: tt.port}
		if got := cfg.Addr(); got != tt.want {
			t.Errorf("Addr() = %q, want %q", got, tt.want)
		}
	}
}

func TestParseConfigFile(t *testing.T) {
	content := `
# this is a comment
host 127.0.0.1
port 6380
password secret
aof yes
aof-file /tmp/test.aof
rdb yes
rdb-file /tmp/test.rdb
max-connections 500
`
	f, err := os.CreateTemp(t.TempDir(), "go-redis-*.conf")
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(content)
	f.Close()

	cfg, err := config.ParseConfigFile(f.Name())
	if err != nil {
		t.Fatalf("ParseConfigFile: %v", err)
	}

	if cfg.Host != "127.0.0.1" {
		t.Errorf("Host: got %q, want 127.0.0.1", cfg.Host)
	}
	if cfg.Port != 6380 {
		t.Errorf("Port: got %d, want 6380", cfg.Port)
	}
	if cfg.Password != "secret" {
		t.Errorf("Password: got %q, want secret", cfg.Password)
	}
	if !cfg.AOFEnabled {
		t.Error("AOFEnabled should be true")
	}
	if cfg.AOFFilename != "/tmp/test.aof" {
		t.Errorf("AOFFilename: got %q", cfg.AOFFilename)
	}
	if !cfg.RDBEnabled {
		t.Error("RDBEnabled should be true")
	}
	if cfg.RDBFilename != "/tmp/test.rdb" {
		t.Errorf("RDBFilename: got %q", cfg.RDBFilename)
	}
	if cfg.MaxConnections != 500 {
		t.Errorf("MaxConnections: got %d, want 500", cfg.MaxConnections)
	}
}

func TestParseConfigFile_SkipsComments(t *testing.T) {
	content := "# host 10.0.0.1\n"
	f, _ := os.CreateTemp(t.TempDir(), "*.conf")
	f.WriteString(content)
	f.Close()

	cfg, err := config.ParseConfigFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	// comment line should be ignored; host stays at default
	if cfg.Host != "0.0.0.0" {
		t.Errorf("Host should remain default, got %q", cfg.Host)
	}
}

func TestParseConfigFile_NotExist(t *testing.T) {
	_, err := config.ParseConfigFile("/nonexistent/path/go-redis.conf")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

func TestParseConfigFile_InvalidPort(t *testing.T) {
	content := "port notanumber\n"
	f, _ := os.CreateTemp(t.TempDir(), "*.conf")
	f.WriteString(content)
	f.Close()

	cfg, err := config.ParseConfigFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	// invalid port should keep default
	if cfg.Port != 6379 {
		t.Errorf("Port should remain 6379 on invalid value, got %d", cfg.Port)
	}
}

func TestParseConfigFile_AofNo(t *testing.T) {
	content := "aof no\n"
	f, _ := os.CreateTemp(t.TempDir(), "*.conf")
	f.WriteString(content)
	f.Close()

	cfg, _ := config.ParseConfigFile(f.Name())
	if cfg.AOFEnabled {
		t.Error("AOFEnabled should be false for 'aof no'")
	}
}
