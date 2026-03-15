package persistence_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jiujuan/go-redis/internal/persistence"
)

// ─────────────────────────────────────────────
//  AOF tests
// ─────────────────────────────────────────────

func tmpFile(t *testing.T, suffix string) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test"+suffix)
}

func TestAOF_CreateAndClose(t *testing.T) {
	path := tmpFile(t, ".aof")
	aof, err := persistence.NewAOF(path, "no")
	if err != nil {
		t.Fatalf("NewAOF: %v", err)
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("AOF file should be created on disk")
	}
}

func TestAOF_Write_SyncNo(t *testing.T) {
	path := tmpFile(t, ".aof")
	aof, _ := persistence.NewAOF(path, "no")
	defer aof.Close()

	if err := aof.Write([]string{"SET", "k", "v"}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := aof.Write([]string{"DEL", "k"}); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func TestAOF_Write_SyncAlways(t *testing.T) {
	path := tmpFile(t, ".aof")
	aof, err := persistence.NewAOF(path, "always")
	if err != nil {
		t.Fatalf("NewAOF always: %v", err)
	}
	defer aof.Close()

	if err := aof.Write([]string{"SET", "x", "1"}); err != nil {
		t.Fatalf("Write always: %v", err)
	}
}

func TestAOF_Write_SyncEverysec(t *testing.T) {
	path := tmpFile(t, ".aof")
	aof, err := persistence.NewAOF(path, "everysec")
	if err != nil {
		t.Fatalf("NewAOF everysec: %v", err)
	}
	aof.Write([]string{"SET", "y", "2"})
	time.Sleep(1100 * time.Millisecond) // let ticker fire once
	if err := aof.Close(); err != nil {
		t.Fatalf("Close everysec: %v", err)
	}
	// file should have been flushed
	info, _ := os.Stat(path)
	if info.Size() == 0 {
		t.Error("AOF file should have content after everysec flush")
	}
}

func TestAOF_Replay(t *testing.T) {
	path := tmpFile(t, ".aof")
	aof, _ := persistence.NewAOF(path, "always")
	aof.Write([]string{"SET", "foo", "bar"})
	aof.Write([]string{"HSET", "myhash", "field", "value"})
	aof.Write([]string{"DEL", "foo"})
	aof.Close()

	var replayed [][]string
	err := persistence.Replay(path, func(args []string) error {
		replayed = append(replayed, args)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(replayed) != 3 {
		t.Fatalf("Replay: got %d commands, want 3", len(replayed))
	}
	if replayed[0][0] != "SET" || replayed[0][1] != "foo" || replayed[0][2] != "bar" {
		t.Errorf("replayed[0]: %v", replayed[0])
	}
	if replayed[1][0] != "HSET" {
		t.Errorf("replayed[1]: %v", replayed[1])
	}
	if replayed[2][0] != "DEL" {
		t.Errorf("replayed[2]: %v", replayed[2])
	}
}

func TestAOF_Replay_NotExist(t *testing.T) {
	// non-existent file should be silently skipped
	err := persistence.Replay("/tmp/definitely_not_exist_aof_file.aof", func([]string) error {
		return nil
	})
	if err != nil {
		t.Errorf("Replay non-existent file should return nil, got %v", err)
	}
}

func TestAOF_Replay_EmptyFile(t *testing.T) {
	path := tmpFile(t, ".aof")
	os.WriteFile(path, []byte{}, 0644)

	var count int
	err := persistence.Replay(path, func([]string) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Replay empty: %v", err)
	}
	if count != 0 {
		t.Errorf("empty AOF replayed %d commands", count)
	}
}

func TestAOF_Replay_LargeDataset(t *testing.T) {
	path := tmpFile(t, ".aof")
	aof, _ := persistence.NewAOF(path, "no")
	const N = 500
	for i := 0; i < N; i++ {
		aof.Write([]string{"SET", "k", "v"})
	}
	aof.Close()

	var count int
	persistence.Replay(path, func([]string) error {
		count++
		return nil
	})
	if count != N {
		t.Errorf("large replay: got %d, want %d", count, N)
	}
}

func TestAOF_Replay_SpecialChars(t *testing.T) {
	path := tmpFile(t, ".aof")
	aof, _ := persistence.NewAOF(path, "always")
	aof.Write([]string{"SET", "key with spaces", "value\nwith\nnewlines"})
	aof.Close()

	var got [][]string
	persistence.Replay(path, func(args []string) error {
		got = append(got, args)
		return nil
	})
	if len(got) != 1 || got[0][1] != "key with spaces" || got[0][2] != "value\nwith\nnewlines" {
		t.Errorf("special chars replay: %v", got)
	}
}

func TestAOF_NewAOF_BadPath(t *testing.T) {
	_, err := persistence.NewAOF("/no/such/directory/test.aof", "no")
	if err == nil {
		t.Error("NewAOF with bad path should return error")
	}
}

// ─────────────────────────────────────────────
//  RDB tests
// ─────────────────────────────────────────────

func TestRDB_SaveAndLoad_Empty(t *testing.T) {
	path := tmpFile(t, ".rdb")
	snap := &persistence.RDBSnapshot{
		CreatedAt: time.Now(),
		Entries:   []*persistence.RDBEntry{},
	}
	if err := persistence.SaveRDB(path, snap); err != nil {
		t.Fatalf("SaveRDB empty: %v", err)
	}
	loaded, err := persistence.LoadRDB(path)
	if err != nil || loaded == nil {
		t.Fatalf("LoadRDB empty: loaded=%v err=%v", loaded, err)
	}
	if len(loaded.Entries) != 0 {
		t.Errorf("empty snapshot: got %d entries", len(loaded.Entries))
	}
}

func TestRDB_SaveAndLoad_StringEntry(t *testing.T) {
	path := tmpFile(t, ".rdb")
	snap := &persistence.RDBSnapshot{
		CreatedAt: time.Now(),
		Entries: []*persistence.RDBEntry{
			{Type: 1, Key: "greeting", Value: &persistence.RDBStringVal{Val: "hello"}},
		},
	}
	if err := persistence.SaveRDB(path, snap); err != nil {
		t.Fatalf("SaveRDB: %v", err)
	}
	loaded, err := persistence.LoadRDB(path)
	if err != nil || loaded == nil {
		t.Fatalf("LoadRDB: %v", err)
	}
	if len(loaded.Entries) != 1 {
		t.Fatalf("entries: got %d, want 1", len(loaded.Entries))
	}
	e := loaded.Entries[0]
	if e.Key != "greeting" || e.Type != 1 {
		t.Errorf("entry key/type: %q %d", e.Key, e.Type)
	}
	sv, ok := e.Value.(*persistence.RDBStringVal)
	if !ok || sv.Val != "hello" {
		t.Errorf("string val: %+v", e.Value)
	}
}

func TestRDB_SaveAndLoad_AllTypes(t *testing.T) {
	path := tmpFile(t, ".rdb")
	snap := &persistence.RDBSnapshot{
		CreatedAt: time.Now(),
		Entries: []*persistence.RDBEntry{
			{Type: 1, Key: "str", Value: &persistence.RDBStringVal{Val: "strval"}},
			{Type: 2, Key: "hash", Value: &persistence.RDBHashVal{Fields: map[string]string{"f": "v"}}},
			{Type: 3, Key: "list", Value: &persistence.RDBListVal{Items: []string{"a", "b"}}},
			{Type: 4, Key: "set", Value: &persistence.RDBSetVal{Members: []string{"x", "y"}}},
			{Type: 5, Key: "zset", Value: &persistence.RDBZSetVal{Members: []string{"m"}, Scores: []float64{1.5}}},
		},
	}
	if err := persistence.SaveRDB(path, snap); err != nil {
		t.Fatalf("SaveRDB all types: %v", err)
	}
	loaded, err := persistence.LoadRDB(path)
	if err != nil || loaded == nil {
		t.Fatalf("LoadRDB all types: %v", err)
	}
	if len(loaded.Entries) != 5 {
		t.Errorf("all types: got %d entries, want 5", len(loaded.Entries))
	}
	// verify hash
	for _, e := range loaded.Entries {
		switch e.Key {
		case "hash":
			hv := e.Value.(*persistence.RDBHashVal)
			if hv.Fields["f"] != "v" {
				t.Errorf("hash field: got %q", hv.Fields["f"])
			}
		case "zset":
			zv := e.Value.(*persistence.RDBZSetVal)
			if len(zv.Members) != 1 || zv.Scores[0] != 1.5 {
				t.Errorf("zset: %+v", zv)
			}
		}
	}
}

func TestRDB_Load_NotExist(t *testing.T) {
	snap, err := persistence.LoadRDB("/tmp/no_such_rdb_file_xyz.rdb")
	if err != nil {
		t.Errorf("LoadRDB non-existent should return (nil, nil), got err=%v", err)
	}
	if snap != nil {
		t.Error("LoadRDB non-existent should return nil snapshot")
	}
}

func TestRDB_Load_Corrupted(t *testing.T) {
	path := tmpFile(t, ".rdb")
	os.WriteFile(path, []byte("this is not valid gob data!!!"), 0644)

	_, err := persistence.LoadRDB(path)
	if err == nil {
		t.Error("LoadRDB corrupted file should return error")
	}
}

func TestRDB_Save_AtomicRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dump.rdb")

	// Save twice to verify atomic rename works
	for i := 0; i < 2; i++ {
		snap := &persistence.RDBSnapshot{
			CreatedAt: time.Now(),
			Entries:   []*persistence.RDBEntry{{Type: 1, Key: "k", Value: &persistence.RDBStringVal{Val: "v"}}},
		}
		if err := persistence.SaveRDB(path, snap); err != nil {
			t.Fatalf("SaveRDB iteration %d: %v", i, err)
		}
		// tmp file should be cleaned up
		if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
			t.Error(".tmp file should not exist after successful save")
		}
	}
}

func TestRDB_Save_BadPath(t *testing.T) {
	err := persistence.SaveRDB("/no/such/dir/dump.rdb", &persistence.RDBSnapshot{})
	if err == nil {
		t.Error("SaveRDB bad path should return error")
	}
}

func TestRDB_CreatedAt_Preserved(t *testing.T) {
	path := tmpFile(t, ".rdb")
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	snap := &persistence.RDBSnapshot{CreatedAt: ts}
	persistence.SaveRDB(path, snap)

	loaded, _ := persistence.LoadRDB(path)
	if !loaded.CreatedAt.Equal(ts) {
		t.Errorf("CreatedAt not preserved: got %v, want %v", loaded.CreatedAt, ts)
	}
}
