package model

import "time"

type Resource struct {
	CPU    int64 `json:"cpu_millis"`
	Memory int64 `json:"memory_mb"`
	Disk   int64 `json:"disk_gb"`
}

func (r Resource) Add(other Resource) Resource {
	return Resource{CPU: r.CPU + other.CPU, Memory: r.Memory + other.Memory, Disk: r.Disk + other.Disk}
}

func (r Resource) Sub(other Resource) Resource {
	return Resource{CPU: r.CPU - other.CPU, Memory: r.Memory - other.Memory, Disk: r.Disk - other.Disk}
}

func (r Resource) Fits(request Resource) bool {
	return r.CPU >= request.CPU && r.Memory >= request.Memory && r.Disk >= request.Disk
}

type Node struct {
	ID            string            `json:"id"`
	Region        string            `json:"region"`
	Zone          string            `json:"zone"`
	Labels        map[string]string `json:"labels,omitempty"`
	Capacity      Resource          `json:"capacity"`
	Allocated     Resource          `json:"allocated"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	Healthy       bool              `json:"healthy"`
}

func (n Node) Available() Resource {
	return n.Capacity.Sub(n.Allocated)
}

type Constraints struct {
	RequiredLabels map[string]string `json:"required_labels,omitempty"`
	Region         string            `json:"region,omitempty"`
	Zone           string            `json:"zone,omitempty"`
}

type JobState string

const (
	Pending   JobState = "pending"
	Running   JobState = "running"
	Failed    JobState = "failed"
	Cancelled JobState = "cancelled"
)

type Job struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Priority    int         `json:"priority"`
	Resources   Resource    `json:"resources"`
	Constraints Constraints `json:"constraints,omitempty"`
	NodeID      string      `json:"node_id,omitempty"`
	State       JobState    `json:"state"`
	Attempts    int         `json:"attempts"`
	MaxAttempts int         `json:"max_attempts"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
	LeaseUntil  *time.Time  `json:"lease_until,omitempty"`
}

type EventType string

const (
	JobSubmitted EventType = "job_submitted"
	JobAssigned  EventType = "job_assigned"
	JobReleased  EventType = "job_released"
	JobFailed    EventType = "job_failed"
	JobCancelled EventType = "job_cancelled"
)

type Event struct {
	Sequence int64     `json:"sequence"`
	Type     EventType `json:"type"`
	JobID    string    `json:"job_id"`
	NodeID   string    `json:"node_id,omitempty"`
	Time     time.Time `json:"time"`
	Reason   string    `json:"reason,omitempty"`
}
