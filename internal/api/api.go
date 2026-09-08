package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/adam-karaki/cloud-resource-scheduler/internal/model"
	"github.com/adam-karaki/cloud-resource-scheduler/internal/scheduler"
	"github.com/adam-karaki/cloud-resource-scheduler/internal/store"
)

type API struct {
	store *store.Store
	eng   *scheduler.Engine
	log   *slog.Logger
}

func New(st *store.Store, eng *scheduler.Engine, logger *slog.Logger) http.Handler {
	a := &API{store: st, eng: eng, log: logger}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /readyz", a.readyz)
	mux.HandleFunc("GET /metrics", a.metrics)

	mux.HandleFunc("POST /v1/nodes", a.createNode)
	mux.HandleFunc("GET /v1/nodes", a.listNodes)
	mux.HandleFunc("POST /v1/nodes/{id}/heartbeat", a.heartbeat)

	mux.HandleFunc("POST /v1/jobs", a.createJob)
	mux.HandleFunc("GET /v1/jobs", a.listJobs)
	mux.HandleFunc("GET /v1/jobs/{id}", a.getJob)
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", a.cancelJob)

	mux.HandleFunc("POST /v1/scheduler/run", a.runScheduler)
	mux.HandleFunc("GET /v1/events", a.events)

	return logging(mux, logger)
}

func (a *API) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) readyz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (a *API) createNode(w http.ResponseWriter, r *http.Request) {
	var node model.Node
	if !decodeJSON(w, r, &node) {
		return
	}
	if node.ID == "" || node.Capacity.CPU <= 0 || node.Capacity.Memory <= 0 || node.Capacity.Disk <= 0 {
		writeError(w, http.StatusBadRequest, "id and positive capacity are required")
		return
	}
	if err := a.store.AddNode(node); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			writeError(w, http.StatusConflict, "node already exists")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, node)
}

func (a *API) listNodes(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.store.ListNodes())
}

func (a *API) heartbeat(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := a.store.HeartbeatNode(id); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func (a *API) createJob(w http.ResponseWriter, r *http.Request) {
	var job model.Job
	if !decodeJSON(w, r, &job) {
		return
	}
	if job.ID == "" || job.Resources.CPU <= 0 || job.Resources.Memory <= 0 || job.Resources.Disk <= 0 {
		writeError(w, http.StatusBadRequest, "id and positive resource requests are required")
		return
	}
	if err := a.store.SubmitJob(job); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			writeError(w, http.StatusConflict, "job already exists")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (a *API) listJobs(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.store.ListJobs())
}

func (a *API) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := a.store.GetJob(r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (a *API) cancelJob(w http.ResponseWriter, r *http.Request) {
	if err := a.store.CancelJob(r.PathValue("id")); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

func (a *API) runScheduler(w http.ResponseWriter, _ *http.Request) {
	count := a.eng.ScheduleOnce()
	writeJSON(w, http.StatusOK, map[string]int{"assignments": count})
}

func (a *API) events(w http.ResponseWriter, r *http.Request) {
	var sequence int64
	if value := r.URL.Query().Get("since"); value != "" {
		_, _ = fmtInt64(value, &sequence)
	}
	writeJSON(w, http.StatusOK, a.store.EventsSince(sequence))
}

func (a *API) metrics(w http.ResponseWriter, _ *http.Request) {
	jobs := a.store.ListJobs()
	nodes := a.store.ListNodes()

	var pending, running, failed int
	for _, job := range jobs {
		switch job.State {
		case model.Pending:
			pending++
		case model.Running:
			running++
		case model.Failed:
			failed++
		}
	}

	var totalCPU, allocatedCPU int64
	for _, node := range nodes {
		totalCPU += node.Capacity.CPU
		allocatedCPU += node.Allocated.CPU
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(
		"# HELP scheduler_pending_jobs Number of pending jobs.\n" +
			"# TYPE scheduler_pending_jobs gauge\n" +
			formatMetric("scheduler_pending_jobs", pending) +
			"# HELP scheduler_running_jobs Number of running jobs.\n" +
			"# TYPE scheduler_running_jobs gauge\n" +
			formatMetric("scheduler_running_jobs", running) +
			"# HELP scheduler_failed_jobs Number of failed jobs.\n" +
			"# TYPE scheduler_failed_jobs gauge\n" +
			formatMetric("scheduler_failed_jobs", failed) +
			"# HELP scheduler_cpu_capacity_millis Total CPU capacity in the cluster.\n" +
			"# TYPE scheduler_cpu_capacity_millis gauge\n" +
			formatMetric("scheduler_cpu_capacity_millis", totalCPU) +
			"# HELP scheduler_cpu_allocated_millis Allocated CPU in the cluster.\n" +
			"# TYPE scheduler_cpu_allocated_millis gauge\n" +
			formatMetric("scheduler_cpu_allocated_millis", allocatedCPU),
	))
}

func logging(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		logger.Debug("http request", "method", r.Method, "path", r.URL.Path)
	})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "state conflict")
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func fmtInt64(value string, out *int64) (int, error) {
	var n int64
	for _, ch := range strings.TrimSpace(value) {
		if ch < '0' || ch > '9' {
			return 0, errors.New("invalid integer")
		}
		n = n*10 + int64(ch-'0')
	}
	*out = n
	return len(value), nil
}

func formatMetric(name string, value any) string {
	return name + " " + fmt.Sprint(value) + "\n"
}
