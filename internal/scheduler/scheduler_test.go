package scheduler

import (
	"log/slog"
	"testing"
	"time"

	"github.com/adam-karaki/cloud-resource-scheduler/internal/model"
	"github.com/adam-karaki/cloud-resource-scheduler/internal/store"
)

func TestSchedulerChoosesFeasibleTightFit(t *testing.T) {
	st := store.New()
	logger := slog.Default()
	engine := New(st, logger, Config{LeaseDuration: time.Minute})

	mustNode(t, st, model.Node{
		ID:       "large",
		Region:   "us-west-2",
		Zone:     "a",
		Capacity: model.Resource{CPU: 8000, Memory: 16384, Disk: 100},
	})
	mustNode(t, st, model.Node{
		ID:       "small",
		Region:   "us-west-2",
		Zone:     "b",
		Capacity: model.Resource{CPU: 2000, Memory: 4096, Disk: 50},
	})

	mustJob(t, st, model.Job{
		ID:        "job-1",
		Priority:  10,
		Resources: model.Resource{CPU: 1000, Memory: 1024, Disk: 10},
	})

	if got := engine.ScheduleOnce(); got != 1 {
		t.Fatalf("assignments = %d, want 1", got)
	}

	job, _ := st.GetJob("job-1")
	if job.NodeID != "small" {
		t.Fatalf("node = %q, want small", job.NodeID)
	}
}

func TestSchedulerHonorsConstraintsAndPriority(t *testing.T) {
	st := store.New()
	engine := New(st, slog.Default(), Config{LeaseDuration: time.Minute})

	mustNode(t, st, model.Node{
		ID: "a", Region: "us-west-2", Zone: "a",
		Labels:   map[string]string{"gpu": "false"},
		Capacity: model.Resource{CPU: 4000, Memory: 8192, Disk: 100},
	})
	mustNode(t, st, model.Node{
		ID: "b", Region: "us-west-2", Zone: "b",
		Labels:   map[string]string{"gpu": "true"},
		Capacity: model.Resource{CPU: 4000, Memory: 8192, Disk: 100},
	})

	mustJob(t, st, model.Job{
		ID: "low", Priority: 1,
		Resources: model.Resource{CPU: 1000, Memory: 512, Disk: 1},
	})
	mustJob(t, st, model.Job{
		ID: "gpu", Priority: 100,
		Resources: model.Resource{CPU: 1000, Memory: 512, Disk: 1},
		Constraints: model.Constraints{
			RequiredLabels: map[string]string{"gpu": "true"},
		},
	})

	engine.ScheduleOnce()

	gpu, _ := st.GetJob("gpu")
	low, _ := st.GetJob("low")

	if gpu.NodeID != "b" {
		t.Fatalf("gpu job node = %q, want b", gpu.NodeID)
	}
	if low.NodeID == "" {
		t.Fatal("low-priority job should also have been scheduled")
	}
}

func TestExpiredLeaseRequeuesWithinAttemptLimit(t *testing.T) {
	st := store.New()
	engine := New(st, slog.Default(), Config{LeaseDuration: 10 * time.Millisecond})

	mustNode(t, st, model.Node{
		ID: "a", Capacity: model.Resource{CPU: 4000, Memory: 8192, Disk: 100},
	})
	mustJob(t, st, model.Job{
		ID: "job", Resources: model.Resource{CPU: 1000, Memory: 512, Disk: 1},
		MaxAttempts: 3,
	})

	engine.ScheduleOnce()
	time.Sleep(20 * time.Millisecond)
	engine.ScheduleOnce()

	job, _ := st.GetJob("job")
	if job.State != model.Running {
		t.Fatalf("state = %s, want running after retry", job.State)
	}
	if job.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", job.Attempts)
	}
}

func mustNode(t *testing.T, st *store.Store, node model.Node) {
	t.Helper()
	if err := st.AddNode(node); err != nil {
		t.Fatal(err)
	}
}

func mustJob(t *testing.T, st *store.Store, job model.Job) {
	t.Helper()
	if err := st.SubmitJob(job); err != nil {
		t.Fatal(err)
	}
}
