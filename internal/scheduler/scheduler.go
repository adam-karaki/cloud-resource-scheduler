package scheduler

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/adam-karaki/cloud-resource-scheduler/internal/model"
	"github.com/adam-karaki/cloud-resource-scheduler/internal/store"
)

type Config struct {
	TickInterval  time.Duration
	LeaseDuration time.Duration
}

type Engine struct {
	store  *store.Store
	logger *slog.Logger
	config Config
}

func New(st *store.Store, logger *slog.Logger, config Config) *Engine {
	if config.TickInterval <= 0 {
		config.TickInterval = 250 * time.Millisecond
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = 30 * time.Second
	}
	return &Engine{store: st, logger: logger, config: config}
}

func (e *Engine) Run(ctx context.Context) {
	ticker := time.NewTicker(e.config.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			e.reapExpired(now)
			e.schedule(now)
		}
	}
}

func (e *Engine) ScheduleOnce() int {
	now := time.Now().UTC()
	e.reapExpired(now)
	return e.schedule(now)
}

func (e *Engine) schedule(now time.Time) int {
	jobs := e.store.ListJobs()
	nodes := e.store.ListNodes()

	pending := make([]model.Job, 0)
	for _, job := range jobs {
		if job.State == model.Pending {
			pending = append(pending, job)
		}
	}
	sort.SliceStable(pending, func(i, j int) bool {
		if pending[i].Priority != pending[j].Priority {
			return pending[i].Priority > pending[j].Priority
		}
		return pending[i].CreatedAt.Before(pending[j].CreatedAt)
	})

	assignments := 0
	for _, job := range pending {
		nodeID, score, ok := e.pickNode(job, nodes)
		if !ok {
			continue
		}

		leaseUntil := now.Add(e.config.LeaseDuration)
		if err := e.store.AssignJob(job.ID, nodeID, leaseUntil); err != nil {
			continue
		}
		assignments++
		e.logger.Info("job scheduled", "job_id", job.ID, "node_id", nodeID, "score", score)

		for i := range nodes {
			if nodes[i].ID == nodeID {
				nodes[i].Allocated = nodes[i].Allocated.Add(job.Resources)
				break
			}
		}
	}
	return assignments
}

func (e *Engine) pickNode(job model.Job, nodes []model.Node) (string, int, bool) {
	bestID := ""
	bestScore := -1

	for _, node := range nodes {
		if !node.Healthy || !matches(job.Constraints, node) {
			continue
		}
		if !node.Available().Fits(job.Resources) {
			continue
		}

		score := scoreNode(job, node)
		if bestID == "" || score > bestScore || (score == bestScore && node.ID < bestID) {
			bestID = node.ID
			bestScore = score
		}
	}

	return bestID, bestScore, bestID != ""
}

func scoreNode(job model.Job, node model.Node) int {
	available := node.Available()

	// Prefer tighter fits to reduce fragmentation, while still leaving
	// enough capacity for the current request.
	cpuWaste := available.CPU - job.Resources.CPU
	memWaste := available.Memory - job.Resources.Memory
	diskWaste := available.Disk - job.Resources.Disk

	score := 100_000
	score -= int(cpuWaste / 100)
	score -= int(memWaste / 16)
	score -= int(diskWaste)

	// Prefer the same region/zone when explicitly requested. The hard
	// constraint is already enforced; this makes the scoring extensible.
	if job.Constraints.Zone != "" && node.Zone == job.Constraints.Zone {
		score += 500
	}
	if job.Constraints.Region != "" && node.Region == job.Constraints.Region {
		score += 250
	}
	return score
}

func matches(c model.Constraints, node model.Node) bool {
	if c.Region != "" && c.Region != node.Region {
		return false
	}
	if c.Zone != "" && c.Zone != node.Zone {
		return false
	}
	for key, expected := range c.RequiredLabels {
		if node.Labels[key] != expected {
			return false
		}
	}
	return true
}

func (e *Engine) reapExpired(now time.Time) {
	for _, job := range e.store.ExpiredLeases(now) {
		if err := e.store.ReleaseJob(job.ID, "lease expired", true); err == nil {
			e.logger.Warn("job lease expired", "job_id", job.ID, "attempts", job.Attempts)
		}
	}
}
