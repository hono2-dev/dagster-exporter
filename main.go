package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "dagster"

// --- GraphQL types ---

type gqlRequest struct {
	Query string `json:"query"`
}

type gqlResponse[T any] struct {
	Data   T      `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

type runsData struct {
	Queued    runsOrError `json:"queued"`
	Started   runsOrError `json:"started"`
	Success   runsOrError `json:"success"`
	Failure   runsOrError `json:"failure"`
	Canceling runsOrError `json:"canceling"`
	Canceled  runsOrError `json:"canceled"`
}

type runsOrError struct {
	Typename string `json:"__typename"`
	Count    int    `json:"count"`
}

type runDurationsData struct {
	RunsOrError struct {
		Typename string      `json:"__typename"`
		Results  []runDetail `json:"results"`
	} `json:"runsOrError"`
}

type runDetail struct {
	JobName   string  `json:"jobName"`
	Status    string  `json:"status"`
	StartTime float64 `json:"startTime"`
	EndTime   float64 `json:"endTime"`
}

type workspaceData struct {
	WorkspaceOrError struct {
		Typename        string          `json:"__typename"`
		LocationEntries []locationEntry `json:"locationEntries"`
	} `json:"workspaceOrError"`
}

type locationEntry struct {
	LocationOrLoadError struct {
		Typename     string       `json:"__typename"`
		Name         string       `json:"name"`
		Repositories []repository `json:"repositories"`
	} `json:"locationOrLoadError"`
}

type repository struct {
	Name      string     `json:"name"`
	Schedules []schedule `json:"schedules"`
	Sensors   []sensor   `json:"sensors"`
}

type schedule struct {
	Name          string      `json:"name"`
	ScheduleState instigState `json:"scheduleState"`
}

type sensor struct {
	Name        string      `json:"name"`
	SensorState instigState `json:"sensorState"`
}

type instigState struct {
	Status string `json:"status"` // RUNNING or STOPPED
	Ticks  []tick `json:"ticks"`
}

type tick struct {
	Status    string  `json:"status"` // SUCCESS, FAILURE, SKIPPED
	Timestamp float64 `json:"timestamp"`
}

// --- Queries ---

const runsQuery = `{
  queued:    runsOrError(filter: {statuses: [QUEUED]})    { __typename ... on Runs { count } }
  started:   runsOrError(filter: {statuses: [STARTED]})   { __typename ... on Runs { count } }
  success:   runsOrError(filter: {statuses: [SUCCESS]})   { __typename ... on Runs { count } }
  failure:   runsOrError(filter: {statuses: [FAILURE]})   { __typename ... on Runs { count } }
  canceling: runsOrError(filter: {statuses: [CANCELING]}) { __typename ... on Runs { count } }
  canceled:  runsOrError(filter: {statuses: [CANCELED]})  { __typename ... on Runs { count } }
}`

func runDurationsQuery(limit int) string {
	return fmt.Sprintf(`{
  runsOrError(filter: {statuses: [SUCCESS, FAILURE, CANCELED]}, limit: %d) {
    __typename
    ... on Runs {
      results { jobName status startTime endTime }
    }
  }
}`, limit)
}

const workspaceQuery = `{
  workspaceOrError {
    __typename
    ... on Workspace {
      locationEntries {
        locationOrLoadError {
          __typename
          ... on RepositoryLocation {
            name
            repositories {
              name
              schedules {
                name
                scheduleState {
                  status
                  ticks(limit: 1) { status timestamp }
                }
              }
              sensors {
                name
                sensorState {
                  status
                  ticks(limit: 1) { status timestamp }
                }
              }
            }
          }
        }
      }
    }
  }
}`

// --- Collector ---

type dagsterCollector struct {
	url       string
	client    *http.Client
	runsLimit int

	runsTotal        *prometheus.Desc
	runLastDuration  *prometheus.Desc
	scheduleRunning  *prometheus.Desc
	scheduleLastTick *prometheus.Desc
	sensorRunning    *prometheus.Desc
	sensorLastTick   *prometheus.Desc
}

func newCollector(url string, runsLimit int) *dagsterCollector {
	return &dagsterCollector{
		url:       url,
		client:    &http.Client{Timeout: 10 * time.Second},
		runsLimit: runsLimit,
		runsTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "runs_total"),
			"Number of Dagster runs by status",
			[]string{"status"}, nil,
		),
		runLastDuration: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "run", "last_duration_seconds"),
			"Duration in seconds of the most recent run per job and status",
			[]string{"job_name", "status"}, nil,
		),
		scheduleRunning: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "schedule", "running"),
			"1 if the schedule is running, 0 if stopped",
			[]string{"schedule_name", "repository", "location"}, nil,
		),
		scheduleLastTick: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "schedule", "last_tick_timestamp_seconds"),
			"Unix timestamp of the schedule's last tick",
			[]string{"schedule_name", "repository", "location"}, nil,
		),
		sensorRunning: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sensor", "running"),
			"1 if the sensor is running, 0 if stopped",
			[]string{"sensor_name", "repository", "location"}, nil,
		),
		sensorLastTick: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sensor", "last_tick_timestamp_seconds"),
			"Unix timestamp of the sensor's last tick",
			[]string{"sensor_name", "repository", "location"}, nil,
		),
	}
}

func (c *dagsterCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.runsTotal
	ch <- c.runLastDuration
	ch <- c.scheduleRunning
	ch <- c.scheduleLastTick
	ch <- c.sensorRunning
	ch <- c.sensorLastTick
}

func (c *dagsterCollector) Collect(ch chan<- prometheus.Metric) {
	if err := c.collectRuns(ch); err != nil {
		slog.Error("failed to collect runs", "err", err)
	}
	if err := c.collectRunDurations(ch); err != nil {
		slog.Error("failed to collect run durations", "err", err)
	}
	if err := c.collectWorkspace(ch); err != nil {
		slog.Error("failed to collect workspace", "err", err)
	}
}

func (c *dagsterCollector) gqlQuery(query string, out any) error {
	body, _ := json.Marshal(gqlRequest{Query: query})
	resp, err := c.client.Post(c.url+"/graphql", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *dagsterCollector) collectRuns(ch chan<- prometheus.Metric) error {
	var resp gqlResponse[runsData]
	if err := c.gqlQuery(runsQuery, &resp); err != nil {
		return err
	}
	for _, pair := range []struct {
		status string
		data   runsOrError
	}{
		{"QUEUED", resp.Data.Queued},
		{"STARTED", resp.Data.Started},
		{"SUCCESS", resp.Data.Success},
		{"FAILURE", resp.Data.Failure},
		{"CANCELING", resp.Data.Canceling},
		{"CANCELED", resp.Data.Canceled},
	} {
		if pair.data.Typename == "Runs" {
			ch <- prometheus.MustNewConstMetric(c.runsTotal, prometheus.GaugeValue, float64(pair.data.Count), pair.status)
		}
	}
	return nil
}

func (c *dagsterCollector) collectRunDurations(ch chan<- prometheus.Metric) error {
	var resp gqlResponse[runDurationsData]
	if err := c.gqlQuery(runDurationsQuery(c.runsLimit), &resp); err != nil {
		return err
	}

	type key struct{ job, status string }
	type entry struct{ duration, endTime float64 }
	latest := make(map[key]entry)

	for _, r := range resp.Data.RunsOrError.Results {
		if r.StartTime == 0 || r.EndTime == 0 {
			continue
		}
		k := key{r.JobName, r.Status}
		e := entry{r.EndTime - r.StartTime, r.EndTime}
		if existing, ok := latest[k]; !ok || e.endTime > existing.endTime {
			latest[k] = e
		}
	}

	for k, e := range latest {
		ch <- prometheus.MustNewConstMetric(c.runLastDuration, prometheus.GaugeValue, e.duration, k.job, k.status)
	}
	return nil
}

func (c *dagsterCollector) collectWorkspace(ch chan<- prometheus.Metric) error {
	var resp gqlResponse[workspaceData]
	if err := c.gqlQuery(workspaceQuery, &resp); err != nil {
		return err
	}
	for _, entry := range resp.Data.WorkspaceOrError.LocationEntries {
		loc := entry.LocationOrLoadError
		if loc.Typename != "RepositoryLocation" {
			continue
		}
		for _, repo := range loc.Repositories {
			for _, s := range repo.Schedules {
				running := boolToFloat(s.ScheduleState.Status == "RUNNING")
				ch <- prometheus.MustNewConstMetric(c.scheduleRunning, prometheus.GaugeValue, running, s.Name, repo.Name, loc.Name)
				if len(s.ScheduleState.Ticks) > 0 {
					ch <- prometheus.MustNewConstMetric(c.scheduleLastTick, prometheus.GaugeValue, s.ScheduleState.Ticks[0].Timestamp, s.Name, repo.Name, loc.Name)
				}
			}
			for _, s := range repo.Sensors {
				running := boolToFloat(s.SensorState.Status == "RUNNING")
				ch <- prometheus.MustNewConstMetric(c.sensorRunning, prometheus.GaugeValue, running, s.Name, repo.Name, loc.Name)
				if len(s.SensorState.Ticks) > 0 {
					ch <- prometheus.MustNewConstMetric(c.sensorLastTick, prometheus.GaugeValue, s.SensorState.Ticks[0].Timestamp, s.Name, repo.Name, loc.Name)
				}
			}
		}
	}
	return nil
}

func boolToFloat(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}

// --- Main ---

func main() {
	addr := flag.String("listen-address", envOrDefault("LISTEN_ADDRESS", ":8000"), "address to listen on")
	dagsterURL := flag.String("dagster-url", envOrDefault("DAGSTER_URL", "http://localhost:3000"), "Dagster webserver URL")
	runsLimit := flag.Int("runs-limit", 200, "number of recent completed runs to fetch for duration metrics")
	flag.Parse()

	prometheus.MustRegister(newCollector(*dagsterURL, *runsLimit))

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	slog.Info("starting dagster-exporter", "addr", *addr, "dagster_url", *dagsterURL)
	if err := http.ListenAndServe(*addr, nil); err != nil {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
