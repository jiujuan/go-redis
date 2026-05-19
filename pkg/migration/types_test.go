package migration

import (
	"testing"
	"time"
)

func TestMigrationStateString(t *testing.T) {
	tests := []struct {
		name  string
		state MigrationState
		want  string
	}{
		{name: "idle", state: StateIdle, want: "idle"},
		{name: "preparing", state: StatePreparing, want: "preparing"},
		{name: "migrating", state: StateMigrating, want: "migrating"},
		{name: "finishing", state: StateFinishing, want: "finishing"},
		{name: "done", state: StateDone, want: "done"},
		{name: "unknown", state: MigrationState(99), want: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.state.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDefaultMigrationConfig(t *testing.T) {
	cfg := DefaultMigrationConfig()
	if cfg == nil {
		t.Fatal("DefaultMigrationConfig returned nil")
	}
	if cfg.BatchSize <= 0 || cfg.Concurrency <= 0 || cfg.RetryLimit < 0 {
		t.Fatalf("invalid defaults: %+v", cfg)
	}
	if cfg.BatchInterval < 0 || cfg.ReadFallbackTimeout < 0 {
		t.Fatalf("invalid timeout defaults: %+v", cfg)
	}
}

func TestNewMigrationContext(t *testing.T) {
	mc := newMigrationContext(11, 3, 2)
	if mc.batchSize != 11 || mc.concurrency != 3 || mc.retryLimit != 2 {
		t.Fatalf("context values = %+v", mc)
	}
	if mc.getState() != StateIdle {
		t.Fatalf("initial state = %s, want idle", mc.getState())
	}
	if mc.cancelCh == nil || mc.doneCh == nil || mc.progressCh == nil {
		t.Fatal("channels must be initialized")
	}
}

func TestMigrationContextStateAndCancel(t *testing.T) {
	mc := newMigrationContext(1, 1, 1)
	mc.setState(StateMigrating)
	if mc.getState() != StateMigrating {
		t.Fatal("setState did not update state")
	}
	if mc.isCancelled() {
		t.Fatal("context should not be cancelled initially")
	}
	close(mc.cancelCh)
	if !mc.isCancelled() {
		t.Fatal("context should report cancelled after close")
	}
}

func TestMigrationTaskAndProgressFields(t *testing.T) {
	task := &MigrationTask{
		ID:          "t1",
		AddedNodes:  []string{"n1"},
		StartedAt:   time.Unix(1, 0),
		FinishedAt:  time.Unix(2, 0),
		TotalKeys:   10,
		MigratedKeys: 7,
		FailedKeys:  2,
		SkippedKeys: 1,
	}
	p := Progress{
		State:        StateDone,
		Task:         task,
		Percent:      70,
		EstimatedETA: 3 * time.Second,
	}
	if p.State != StateDone || p.Task != task || p.Percent != 70 || p.EstimatedETA != 3*time.Second {
		t.Fatalf("progress mismatch: %+v", p)
	}
}

func TestMigrationConfigFieldSanity(t *testing.T) {
	cfg := &MigrationConfig{
		BatchSize:           1,
		Concurrency:         2,
		RetryLimit:          3,
		BatchInterval:       4 * time.Millisecond,
		ReadFallbackTimeout: 5 * time.Millisecond,
	}
	if cfg.BatchSize != 1 || cfg.Concurrency != 2 || cfg.RetryLimit != 3 {
		t.Fatalf("config mismatch: %+v", cfg)
	}
}
