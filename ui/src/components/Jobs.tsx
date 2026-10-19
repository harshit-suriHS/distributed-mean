import { useState } from "react";
import { api, type Job, type JobSummary } from "../api";

function JobRow({ job }: { job: JobSummary }) {
  const [details, setDetails] = useState<Job | null>(null);
  const [open, setOpen] = useState(false);

  const toggle = async () => {
    setOpen(!open);
    if (!open) setDetails(await api.job(job.id));
  };
  const download = async () => {
    const { url } = await api.result(job.id);
    window.open(url, "_blank");
  };

  return (
    <>
      <tr onClick={toggle} className="clickable">
        <td className="mono">{job.id.slice(-8)}</td>
        <td>
          <span className={`badge ${job.status}`}>{job.status}</span>
        </td>
        <td>
          {job.no_of_files} × {job.no_of_columns}
        </td>
        <td>
          <div className="bar">
            <div className={`fill ${job.status}`} style={{ width: `${job.percent}%` }} />
          </div>
        </td>
        <td className="muted">{new Date(job.created_at).toLocaleTimeString()}</td>
        <td>
          {job.status === "succeeded" && (
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                void download();
              }}
            >
              result
            </button>
          )}
        </td>
      </tr>
      {open && details && (
        <tr className="details">
          <td colSpan={6}>
            tasks:{" "}
            {Object.entries(details.tasks)
              .map(([k, v]) => `${k} ${v}`)
              .join(" · ")}
            {details.error && (
              <div className="error">
                {details.failure_type}: {details.error}
              </div>
            )}
          </td>
        </tr>
      )}
    </>
  );
}

export function Jobs({ jobs }: { jobs: JobSummary[] }) {
  return (
    <>
      <h2>Jobs</h2>
      {jobs.length === 0 ? (
        <p className="muted">no jobs yet: submit one</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>job</th>
              <th>status</th>
              <th>F × C</th>
              <th>progress</th>
              <th>created</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => (
              <JobRow key={job.id} job={job} />
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}
