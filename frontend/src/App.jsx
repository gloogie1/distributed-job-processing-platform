import { useEffect, useMemo, useState } from "react";
import "./App.css";

const API_BASE_URL = "http://localhost:8080";

const PRESET_FILES = [
  {
    label: "Small sample CSV",
    filePath: "/data/sample_trips.csv",
    chunkSize: 2,
  },
  {
    label: "NYC Yellow Taxi Jan 2024",
    filePath: "/data/yellow_tripdata_2024-01.csv",
    chunkSize: 5000,
  },
];

function App() {
  const [jobs, setJobs] = useState([]);
  const [selectedJobId, setSelectedJobId] = useState("");
  const [selectedJob, setSelectedJob] = useState(null);
  const [chunks, setChunks] = useState([]);
  const [errors, setErrors] = useState([]);

  const [filePath, setFilePath] = useState(PRESET_FILES[0].filePath);
  const [chunkSize, setChunkSize] = useState(PRESET_FILES[0].chunkSize);
  const [presetLabel, setPresetLabel] = useState(PRESET_FILES[0].label);

  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");

  const dashboardStats = useMemo(() => {
    const totalJobs = jobs.length;
    const runningJobs = jobs.filter((job) => job.status === "RUNNING").length;
    const completedJobs = jobs.filter((job) => job.status === "COMPLETED").length;
    const errorJobs = jobs.filter((job) =>
      ["FAILED", "COMPLETED_WITH_ERRORS"].includes(job.status)
    ).length;

    return {
      totalJobs,
      runningJobs,
      completedJobs,
      errorJobs,
    };
  }, [jobs]);

  const chunkStats = useMemo(() => {
    const byWorker = {};
    const byStatus = {};

    for (const chunk of chunks) {
      const worker = chunk.workerId || "unassigned";
      byWorker[worker] = (byWorker[worker] || 0) + 1;
      byStatus[chunk.status] = (byStatus[chunk.status] || 0) + 1;
    }

    return { byWorker, byStatus };
  }, [chunks]);

  async function apiGet(path) {
    const response = await fetch(`${API_BASE_URL}${path}`);
    if (!response.ok) {
      throw new Error(`GET ${path} failed with ${response.status}`);
    }
    return response.json();
  }

  async function loadJobs() {
    try {
      const data = await apiGet("/jobs");
      setJobs(data);

      if (!selectedJobId && data.length > 0) {
        selectJob(data[0].id);
      }
    } catch (error) {
      setMessage(error.message);
    }
  }

  async function selectJob(jobId) {
    setSelectedJobId(jobId);
    setChunks([]);
    setErrors([]);

    try {
      const job = await apiGet(`/jobs/${jobId}`);
      setSelectedJob(job);

      const chunkData = await apiGet(`/jobs/${jobId}/chunks`);
      setChunks(chunkData);

      const errorData = await apiGet(`/jobs/${jobId}/errors`);
      setErrors(errorData);

      setMessage(`Loaded job ${jobId}`);
    } catch (error) {
      setMessage(error.message);
    }
  }

  async function refreshSelectedJob() {
    if (!selectedJobId) return;

    try {
      const job = await apiGet(`/jobs/${selectedJobId}`);
      setSelectedJob(job);

      const chunkData = await apiGet(`/jobs/${selectedJobId}/chunks`);
      setChunks(chunkData);

      setMessage("Job refreshed.");
    } catch (error) {
      setMessage(error.message);
    }
  }

  async function submitJob(event) {
    event.preventDefault();
    setLoading(true);
    setMessage("Submitting job...");

    try {
      const response = await fetch(`${API_BASE_URL}/jobs`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          filePath,
          chunkSize: Number(chunkSize),
        }),
      });

      if (!response.ok) {
        throw new Error(`POST /jobs failed with ${response.status}`);
      }

      const job = await response.json();

      setSelectedJobId(job.id);
      setSelectedJob(job);
      setChunks([]);
      setErrors([]);
      setMessage(`Submitted job ${job.id}`);

      await loadJobs();
      await selectJob(job.id);
    } catch (error) {
      setMessage(error.message);
    } finally {
      setLoading(false);
    }
  }

  function applyPreset(label) {
    const preset = PRESET_FILES.find((item) => item.label === label);
    if (!preset) return;

    setPresetLabel(label);
    setFilePath(preset.filePath);
    setChunkSize(preset.chunkSize);
  }

  useEffect(() => {
    loadJobs();
  }, []);

  useEffect(() => {
    if (!selectedJobId) return;

    const intervalId = setInterval(() => {
      refreshSelectedJob();
      loadJobs();
    }, 5000);

    return () => clearInterval(intervalId);
  }, [selectedJobId]);

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">DJP</div>
          <div>
            <h1>Job Control</h1>
            <p>Distributed processing dashboard</p>
          </div>
        </div>

        <button className="secondary full-width" onClick={loadJobs}>
          Refresh Jobs
        </button>

        <div className="job-list">
          {jobs.map((job) => (
            <button
              key={job.id}
              className={`job-list-item ${selectedJobId === job.id ? "active" : ""
                }`}
              onClick={() => selectJob(job.id)}
            >
              <span className={`status-dot ${job.status.toLowerCase()}`} />
              <div>
                <strong>{job.status}</strong>
                <small>{job.id}</small>
              </div>
            </button>
          ))}
        </div>
      </aside>

      <section className="main-panel">
        <header className="topbar">
          <div>
            <h2>Distributed Job Processing Platform</h2>
            <p>
              Submit jobs, monitor chunk execution, inspect worker distribution,
              and review validation results.
            </p>
          </div>

          <div className="topbar-actions">
            <button onClick={refreshSelectedJob}>Refresh Selected</button>
          </div>
        </header>

        <section className="stats-grid">
          <MetricCard label="Recent Jobs" value={dashboardStats.totalJobs} />
          <MetricCard label="Running" value={dashboardStats.runningJobs} />
          <MetricCard label="Completed" value={dashboardStats.completedJobs} />
          <MetricCard label="Errors" value={dashboardStats.errorJobs} />
        </section>

        <section className="content-grid">
          <section className="card submit-card">
            <div className="section-heading">
              <h3>Submit New Job</h3>
              <p>Create a file-processing job and publish chunks to Kafka.</p>
            </div>

            <form onSubmit={submitJob} className="job-form">
              <label>
                Preset
                <select
                  value={presetLabel}
                  onChange={(event) => applyPreset(event.target.value)}
                >
                  {PRESET_FILES.map((preset) => (
                    <option key={preset.label} value={preset.label}>
                      {preset.label}
                    </option>
                  ))}
                </select>
              </label>

              <label>
                File path
                <input
                  value={filePath}
                  onChange={(event) => setFilePath(event.target.value)}
                />
              </label>

              <label>
                Chunk size
                <input
                  type="number"
                  min="1"
                  value={chunkSize}
                  onChange={(event) => setChunkSize(event.target.value)}
                />
              </label>

              <button type="submit" disabled={loading}>
                {loading ? "Submitting..." : "Run Job"}
              </button>
            </form>

            {message && <p className="message">{message}</p>}
          </section>

          {selectedJob && (
            <section className="card">
              <div className="section-heading">
                <h3>Selected Job</h3>
                <p>{selectedJob.id}</p>
              </div>

              <div className="job-summary">
                {(() => {
                  const durationSeconds = getDurationSeconds(
                    selectedJob.createdAt,
                    selectedJob.completedAt
                  );

                  return (
                    <>
                      <MetricCard label="Status" value={selectedJob.status} />
                      <MetricCard label="Rows" value={selectedJob.totalRows.toLocaleString()} />
                      <MetricCard label="Valid" value={selectedJob.validRows.toLocaleString()} />
                      <MetricCard label="Invalid" value={selectedJob.invalidRows.toLocaleString()} />
                      <MetricCard label="Invalid %" value={formatPercent(selectedJob.invalidRows, selectedJob.totalRows)} />
                      <MetricCard label="Chunks" value={selectedJob.totalChunks.toLocaleString()} />
                      <MetricCard label="Completed Chunks" value={selectedJob.completedChunks.toLocaleString()} />
                      <MetricCard label="Failed Chunks" value={selectedJob.failedChunks.toLocaleString()} />
                      <MetricCard label="Started At" value={formatDateTime(selectedJob.createdAt)} />
                      <MetricCard label="Completed At" value={formatDateTime(selectedJob.completedAt)} />
                      <MetricCard label="Duration" value={formatDuration(durationSeconds)} />
                      <MetricCard
                        label="Rows/sec"
                        value={formatRate(selectedJob.totalRows, durationSeconds, "rows/sec")}
                      />
                      <MetricCard
                        label="Chunks/sec"
                        value={formatRate(selectedJob.completedChunks, durationSeconds, "chunks/sec")}
                      />
                    </>
                  );
                })()}
              </div>

              <ProgressBar
                completed={selectedJob.completedChunks}
                total={selectedJob.totalChunks}
              />
            </section>
          )}
        </section>

        {chunks.length > 0 && (
          <section className="content-grid">
            <section className="card">
              <div className="section-heading">
                <h3>Worker Distribution</h3>
                <p>Chunks processed by each worker instance.</p>
              </div>

              <Breakdown data={chunkStats.byWorker} />
            </section>

            <section className="card">
              <div className="section-heading">
                <h3>Chunk Status</h3>
                <p>Current execution state across all chunks.</p>
              </div>

              <Breakdown data={chunkStats.byStatus} />
            </section>
          </section>
        )}

        {chunks.length > 0 && (
          <section className="card">
            <div className="section-heading">
              <h3>Chunks</h3>
              <p>Showing {chunks.length} chunks ordered by start row.</p>
            </div>

            <div className="table-wrapper">
              <table>
                <thead>
                  <tr>
                    <th>Start</th>
                    <th>End</th>
                    <th>Status</th>
                    <th>Worker</th>
                    <th>Valid</th>
                    <th>Invalid</th>
                    <th>Retry</th>
                    <th>Chunk File</th>
                  </tr>
                </thead>
                <tbody>
                  {chunks.map((chunk) => (
                    <tr key={chunk.id}>
                      <td>{chunk.startRow}</td>
                      <td>{chunk.endRow}</td>
                      <td>
                        <span className={`pill ${chunk.status.toLowerCase()}`}>
                          {chunk.status}
                        </span>
                      </td>
                      <td>{chunk.workerId || "-"}</td>
                      <td>{chunk.validRows}</td>
                      <td>{chunk.invalidRows}</td>
                      <td>{chunk.retryCount}</td>
                      <td className="path-cell">{chunk.chunkFilePath}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}

        {errors.length > 0 && (
          <section className="card">
            <div className="section-heading">
              <h3>Validation Error Samples</h3>
              <p>
                Showing {Math.min(errors.length, 500)} of {errors.length} stored
                validation errors.
              </p>
            </div>

            <div className="table-wrapper">
              <table>
                <thead>
                  <tr>
                    <th>Row</th>
                    <th>Field</th>
                    <th>Invalid Value</th>
                    <th>Error Code</th>
                    <th>Message</th>
                  </tr>
                </thead>
                <tbody>
                  {errors.slice(0, 500).map((error) => (
                    <tr key={error.id}>
                      <td>{error.rowNumber}</td>
                      <td>{error.fieldName}</td>
                      <td>{error.invalidValue}</td>
                      <td>{error.errorCode}</td>
                      <td>{error.errorMessage}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}
      </section>
    </main>
  );
}

function formatDateTime(value) {
  if (!value) return "-";
  return new Date(value).toLocaleString();
}

function getDurationSeconds(start, end) {
  if (!start) return null;

  const startMs = new Date(start).getTime();
  const endMs = end ? new Date(end).getTime() : Date.now();

  if (Number.isNaN(startMs) || Number.isNaN(endMs)) return null;

  return Math.max(0, Math.round((endMs - startMs) / 1000));
}

function formatDuration(seconds) {
  if (seconds === null) return "-";

  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;

  if (minutes === 0) {
    return `${remainingSeconds}s`;
  }

  return `${minutes}m ${remainingSeconds}s`;
}

function formatRate(numerator, seconds, suffix) {
  if (!seconds || seconds <= 0) return "-";
  return `${Math.round(numerator / seconds).toLocaleString()} ${suffix}`;
}

function formatPercent(numerator, denominator) {
  if (!denominator || denominator <= 0) return "-";
  return `${((numerator / denominator) * 100).toFixed(2)}%`;
}

function MetricCard({ label, value }) {
  return (
    <div className="metric-card">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function ProgressBar({ completed, total }) {
  const percentage = total > 0 ? Math.round((completed / total) * 100) : 0;

  return (
    <div className="progress-block">
      <div className="progress-header">
        <span>Progress</span>
        <strong>{percentage}%</strong>
      </div>
      <div className="progress-track">
        <div className="progress-fill" style={{ width: `${percentage}%` }} />
      </div>
    </div>
  );
}

function Breakdown({ data }) {
  const entries = Object.entries(data);

  if (entries.length === 0) {
    return <p className="message">No data available.</p>;
  }

  return (
    <div className="breakdown">
      {entries.map(([key, value]) => (
        <div key={key} className="breakdown-row">
          <span>{key}</span>
          <strong>{value}</strong>
        </div>
      ))}
    </div>
  );
}


export default App;