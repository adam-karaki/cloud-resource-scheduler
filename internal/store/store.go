package store

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/adam-karaki/cloud-resource-scheduler/internal/model"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrConflict      = errors.New("state conflict")
)

type Store struct {
	mu     sync.RWMutex
	nodes  map[string]*model.Node
	jobs   map[string]*model.Job
	events []model.Event
	seq    int64
}

func New() *Store {
	return &Store{
		nodes: make(map[string]*model.Node),
		jobs:  make(map[string]*model.Job),
	}
}

func (s *Store) AddNode(node model.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.nodes[node.ID]; ok {
		return ErrAlreadyExists
	}
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	node.LastHeartbeat = time.Now().UTC()
	node.Healthy = true
	s.nodes[node.ID] = &node
	return nil
}

func (s *Store) HeartbeatNode(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.nodes[id]
	if !ok {
		return ErrNotFound
	}
	node.LastHeartbeat = time.Now().UTC()
	node.Healthy = true
	return nil
}

func (s *Store) ListNodes() []model.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]model.Node, 0, len(s.nodes))
	for _, node := range s.nodes {
		copy := *node
		copy.Labels = cloneMap(node.Labels)
		out = append(out, copy)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *Store) GetNode(id string) (model.Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	node, ok := s.nodes[id]
	if !ok {
		return model.Node{}, ErrNotFound
	}
	copy := *node
	copy.Labels = cloneMap(node.Labels)
	return copy, nil
}

func (s *Store) SubmitJob(job model.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.jobs[job.ID]; ok {
		return ErrAlreadyExists
	}
	now := time.Now().UTC()
	job.CreatedAt = now
	job.UpdatedAt = now
	job.State = model.Pending
	if job.MaxAttempts <= 0 {
		job.MaxAttempts = 3
	}
	s.jobs[job.ID] = &job
	s.appendEventLocked(model.Event{Type: model.JobSubmitted, JobID: job.ID, Time: now})
	return nil
}

func (s *Store) ListJobs() []model.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]model.Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		copy := *job
		out = append(out, copy)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

func (s *Store) GetJob(id string) (model.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[id]
	if !ok {
		return model.Job{}, ErrNotFound
	}
	return *job, nil
}

func (s *Store) AssignJob(jobID, nodeID string, leaseUntil time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return ErrNotFound
	}
	node, ok := s.nodes[nodeID]
	if !ok {
		return ErrNotFound
	}
	if job.State != model.Pending {
		return ErrConflict
	}
	if !node.Healthy || !node.Available().Fits(job.Resources) {
		return ErrConflict
	}

	node.Allocated = node.Allocated.Add(job.Resources)
	job.NodeID = nodeID
	job.State = model.Running
	job.Attempts++
	job.LeaseUntil = &leaseUntil
	job.UpdatedAt = time.Now().UTC()

	s.appendEventLocked(model.Event{
		Type:   model.JobAssigned,
		JobID:  jobID,
		NodeID: nodeID,
		Time:   job.UpdatedAt,
	})
	return nil
}

func (s *Store) ReleaseJob(jobID string, reason string, retry bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return ErrNotFound
	}

	if job.NodeID != "" {
		if node, exists := s.nodes[job.NodeID]; exists {
			node.Allocated = node.Allocated.Sub(job.Resources)
		}
	}

	job.NodeID = ""
	job.LeaseUntil = nil
	job.UpdatedAt = time.Now().UTC()

	if retry && job.Attempts < job.MaxAttempts {
		job.State = model.Pending
	} else {
		job.State = model.Failed
	}

	s.appendEventLocked(model.Event{
		Type:   model.JobReleased,
		JobID:  jobID,
		NodeID: job.NodeID,
		Time:   job.UpdatedAt,
		Reason: reason,
	})
	return nil
}

func (s *Store) CancelJob(jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return ErrNotFound
	}
	if job.State == model.Running && job.NodeID != "" {
		if node, exists := s.nodes[job.NodeID]; exists {
			node.Allocated = node.Allocated.Sub(job.Resources)
		}
	}
	job.NodeID = ""
	job.LeaseUntil = nil
	job.State = model.Cancelled
	job.UpdatedAt = time.Now().UTC()
	s.appendEventLocked(model.Event{Type: model.JobCancelled, JobID: jobID, Time: job.UpdatedAt})
	return nil
}

func (s *Store) ExpiredLeases(now time.Time) []model.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []model.Job
	for _, job := range s.jobs {
		if job.State == model.Running && job.LeaseUntil != nil && now.After(*job.LeaseUntil) {
			out = append(out, *job)
		}
	}
	return out
}

func (s *Store) EventsSince(sequence int64) []model.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []model.Event
	for _, event := range s.events {
		if event.Sequence > sequence {
			out = append(out, event)
		}
	}
	return out
}

func (s *Store) appendEventLocked(event model.Event) {
	s.seq++
	event.Sequence = s.seq
	s.events = append(s.events, event)
}

func cloneMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	out := make(map[string]string, len(input))
	for k, v := range input {
		out[k] = v
	}
	return out
}
