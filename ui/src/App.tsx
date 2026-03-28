import { startTransition, useDeferredValue, useEffect, useEffectEvent, useState } from "react";
import { BrowserRouter, NavLink, Navigate, Route, Routes } from "react-router-dom";
import { fetchKeyLocation, fetchScenarioRun, fetchScenarioRuns } from "./lib/api";
import { useClusterSnapshot } from "./hooks/useClusterSnapshot";
import { useEventStream } from "./hooks/useEventStream";
import type {
  ClusterEvent,
  ClusterSnapshot,
  KeyLocationView,
  NodeView,
  RangeView,
  ScenarioRunDetail,
  ScenarioRunView,
} from "./types";

const snapshotRefreshMs = 2500;

export function App() {
  const snapshotState = useClusterSnapshot(snapshotRefreshMs);
  const streamState = useEventStream(snapshotState.snapshot?.events ?? [], 64, 256);
  const snapshot = snapshotState.snapshot;

  return (
    <BrowserRouter>
      <div className="console-shell">
        <div className="background-orbit background-orbit-left" />
        <div className="background-orbit background-orbit-right" />
        <aside className="console-rail">
          <div>
            <p className="eyebrow">Chronos Console</p>
            <h1>Distributed Operations</h1>
            <p className="rail-copy">
              Live cluster topology, placement, and event visibility for ChronosDB.
            </p>
          </div>
          <nav className="console-nav" aria-label="Primary">
            <NavItem to="/overview" label="Overview" subtitle="State and signals" />
            <NavItem to="/nodes" label="Nodes" subtitle="Health and residency" />
            <NavItem to="/ranges" label="Ranges" subtitle="Descriptors and placement" />
            <NavItem to="/events" label="Events" subtitle="Live operations stream" />
            <NavItem to="/scenarios" label="Scenarios" subtitle="Retained chaos runs" />
          </nav>
          <div className="rail-footer">
            <StatusPill
              tone={streamState.status === "live" ? "good" : "warn"}
              label={`stream ${streamState.status}`}
            />
            <StatusPill
              tone={snapshotState.error ? "bad" : "neutral"}
              label={snapshotState.error ? "snapshot degraded" : "snapshot healthy"}
            />
          </div>
        </aside>
        <main className="console-main">
          <header className="console-header">
            <div>
              <p className="eyebrow">Cluster Surface</p>
              <h2>{snapshot ? clusterHeadline(snapshot) : "Waiting for Chronos cluster state"}</h2>
            </div>
            <div className="header-actions">
              <button className="refresh-button" type="button" onClick={snapshotState.refresh}>
                Refresh
              </button>
              <div className="header-meta">
                <span>snapshot</span>
                <strong>{formatInstant(snapshotState.lastUpdatedAt)}</strong>
              </div>
              <div className="header-meta">
                <span>stream</span>
                <strong>{streamState.status}</strong>
              </div>
            </div>
          </header>

          {snapshotState.error ? (
            <section className="banner banner-error">
              <strong>Snapshot error:</strong> {snapshotState.error}
            </section>
          ) : null}
          {streamState.error ? (
            <section className="banner banner-warning">
              <strong>Stream notice:</strong> {streamState.error}
            </section>
          ) : null}

          <Routes>
            <Route
              path="/overview"
              element={
                <OverviewPage
                  loading={snapshotState.loading}
                  snapshot={snapshot}
                  events={streamState.events}
                />
              }
            />
            <Route path="/nodes" element={<NodesPage nodes={snapshot?.nodes ?? []} />} />
            <Route path="/ranges" element={<RangesPage nodes={snapshot?.nodes ?? []} ranges={snapshot?.ranges ?? []} />} />
            <Route path="/events" element={<EventsPage events={streamState.events} />} />
            <Route path="/scenarios" element={<ScenariosPage />} />
            <Route path="*" element={<Navigate to="/overview" replace />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}

function NavItem(props: { to: string; label: string; subtitle: string }) {
  return (
    <NavLink className={({ isActive }) => `nav-item${isActive ? " nav-item-active" : ""}`} to={props.to}>
      <strong>{props.label}</strong>
      <span>{props.subtitle}</span>
    </NavLink>
  );
}

function OverviewPage(props: {
  loading: boolean;
  snapshot: ClusterSnapshot | null;
  events: ClusterEvent[];
}) {
  const snapshot = props.snapshot;
  if (!snapshot && props.loading) {
    return <section className="panel">Loading cluster snapshot…</section>;
  }
  if (!snapshot) {
    return <section className="panel">No cluster snapshot available.</section>;
  }

  const degradedNodes = snapshot.nodes.filter((node) => node.status !== "ok");
  const leaseholderSpread = new Set(
    snapshot.ranges
      .map((range) => range.leaseholder_node_id)
      .filter((nodeID): nodeID is number => typeof nodeID === "number" && nodeID > 0),
  );
  const recentEvents = props.events.slice(Math.max(props.events.length - 6, 0)).reverse();

  return (
    <div className="page-grid">
      <section className="stats-grid">
        <MetricCard label="Nodes" value={snapshot.nodes.length} hint={`${degradedNodes.length} degraded`} />
        <MetricCard label="Ranges" value={snapshot.ranges.length} hint="authoritative descriptors" />
        <MetricCard label="Leaseholder spread" value={leaseholderSpread.size} hint="nodes currently owning leases" />
        <MetricCard label="Recent events" value={props.events.length} hint="retained stream window" />
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Cluster posture</p>
            <h3>Node operating surface</h3>
          </div>
        </div>
        <div className="node-summary-grid">
          {snapshot.nodes.map((node) => (
            <article className="node-summary-card" key={node.node_id}>
              <div className="node-summary-header">
                <strong>node {node.node_id}</strong>
                <StatusPill tone={node.status === "ok" ? "good" : "warn"} label={node.status} />
              </div>
              <dl className="metric-list">
                <div>
                  <dt>replicas</dt>
                  <dd>{node.replica_count}</dd>
                </div>
                <div>
                  <dt>leases</dt>
                  <dd>{node.lease_count}</dd>
                </div>
                <div>
                  <dt>started</dt>
                  <dd>{formatInstant(node.started_at)}</dd>
                </div>
              </dl>
            </article>
          ))}
        </div>
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Recent activity</p>
            <h3>Live operations window</h3>
          </div>
        </div>
        <div className="event-stack">
          {recentEvents.length === 0 ? <EmptyState label="No events received yet." /> : null}
          {recentEvents.map((event) => (
            <EventRow event={event} key={event.id ?? `${event.timestamp}-${event.type}-${event.message}`} />
          ))}
        </div>
      </section>
    </div>
  );
}

function NodesPage(props: { nodes: NodeView[] }) {
  return (
    <section className="page-grid">
      <div className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Nodes</p>
            <h3>Health, endpoints, and residency counts</h3>
          </div>
        </div>
        <div className="node-card-grid">
          {props.nodes.length === 0 ? <EmptyState label="No nodes surfaced by the console API." /> : null}
          {props.nodes.map((node) => (
            <article className="node-card" key={node.node_id}>
              <div className="node-card-header">
                <div>
                  <p className="node-id">node {node.node_id}</p>
                  <StatusPill tone={node.status === "ok" ? "good" : "warn"} label={node.status} />
                </div>
                <div className="endpoint-list">
                  <span>{node.pg_addr ?? "pgwire unknown"}</span>
                  <span>{node.observability_url ?? "observability unknown"}</span>
                </div>
              </div>
              <dl className="metric-list">
                <div>
                  <dt>replicas</dt>
                  <dd>{node.replica_count}</dd>
                </div>
                <div>
                  <dt>leases</dt>
                  <dd>{node.lease_count}</dd>
                </div>
                <div>
                  <dt>partitions</dt>
                  <dd>{node.partitioned_from?.length ? node.partitioned_from.join(", ") : "none"}</dd>
                </div>
                <div>
                  <dt>started</dt>
                  <dd>{formatInstant(node.started_at)}</dd>
                </div>
              </dl>
              {node.notes?.length ? (
                <div className="notes-block">
                  {node.notes.map((note) => (
                    <p key={note}>{note}</p>
                  ))}
                </div>
              ) : null}
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}

function RangesPage(props: { nodes: NodeView[]; ranges: RangeView[] }) {
  const [filter, setFilter] = useState("");
  const [selectedRangeID, setSelectedRangeID] = useState<number | null>(null);
  const [lookupKey, setLookupKey] = useState("");
  const [lookupResult, setLookupResult] = useState<KeyLocationView | null>(null);
  const [lookupError, setLookupError] = useState<string | null>(null);
  const [lookupLoading, setLookupLoading] = useState(false);
  const deferredFilter = useDeferredValue(filter.trim().toLowerCase());
  const filtered = props.ranges.filter((range) => matchesRange(range, deferredFilter));
  const selectedRange = filtered.find((range) => range.range_id === selectedRangeID) ?? filtered[0] ?? null;
  const placementNodes = placementNodesForRange(props.nodes, selectedRange);

  return (
    <section className="page-grid">
      <div className="range-layout">
        <div className="panel">
          <div className="panel-header panel-header-with-control">
            <div>
              <p className="eyebrow">Ranges</p>
              <h3>Descriptors, replicas, and leaseholders</h3>
            </div>
            <label className="filter-box">
              <span>Filter</span>
              <input
                aria-label="Filter ranges"
                onChange={(event) => setFilter(event.target.value)}
                placeholder="range id, node id, key prefix, placement"
                value={filter}
              />
            </label>
          </div>
          {filtered.length === 0 ? <EmptyState label="No ranges match the current filter." /> : null}
          {filtered.length > 0 ? (
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>range</th>
                    <th>keys</th>
                    <th>leaseholder</th>
                    <th>replicas</th>
                    <th>placement</th>
                    <th>source</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((range) => (
                    <tr
                      className={range.range_id === selectedRange?.range_id ? "data-row-selected" : ""}
                      key={range.range_id}
                      onClick={() => setSelectedRangeID(range.range_id)}
                    >
                      <td>
                        <strong>{range.range_id}</strong>
                        <span className="subtle-mono">gen {range.generation}</span>
                      </td>
                      <td className="subtle-mono">
                        {range.start_key || "∅"} .. {range.end_key || "∞"}
                      </td>
                      <td>{range.leaseholder_node_id ? `node ${range.leaseholder_node_id}` : "unknown"}</td>
                      <td>
                        <div className="replica-chip-row">
                          {range.replicas.map((replica) => (
                            <span className="replica-chip" key={replica.replica_id}>
                              n{replica.node_id}:{replica.role}
                            </span>
                          ))}
                        </div>
                      </td>
                      <td>
                        <strong>{range.placement_mode ?? "unplaced"}</strong>
                        {range.preferred_regions?.length ? (
                          <span className="subtle-copy">{range.preferred_regions.join(", ")}</span>
                        ) : null}
                      </td>
                      <td>{range.source ?? "unknown"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}
        </div>

        <div className="panel placement-panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Placement</p>
                <h3>Leaseholder and replica residency</h3>
              </div>
              {selectedRange ? (
                <StatusPill
                  label={`range ${selectedRange.range_id}`}
                  tone={selectedRange.placement_mode ? "good" : "neutral"}
                />
              ) : null}
            </div>
            <form
              className="lookup-form"
              onSubmit={(event) => {
                event.preventDefault();
                if (!lookupKey.trim()) {
                  startTransition(() => {
                    setLookupError("Enter a key to locate.");
                    setLookupResult(null);
                  });
                  return;
                }
                setLookupLoading(true);
                void fetchKeyLocation(lookupKey.trim())
                  .then((location) => {
                    startTransition(() => {
                      setLookupLoading(false);
                      setLookupError(null);
                      setLookupResult(location);
                      setFilter("");
                      setSelectedRangeID(location.range.range_id);
                    });
                  })
                  .catch((error) => {
                    const message = error instanceof Error ? error.message : "key lookup failed";
                    startTransition(() => {
                      setLookupLoading(false);
                      setLookupResult(null);
                      setLookupError(message);
                    });
                  });
              }}
            >
              <label className="filter-box lookup-form-field">
                <span>Locate key</span>
                <div className="lookup-form-row">
                  <input
                    aria-label="Locate logical key"
                    onChange={(event) => setLookupKey(event.target.value)}
                    placeholder="customer/42 or hex:637573746f6d65722f3432"
                    value={lookupKey}
                  />
                  <button className="refresh-button" disabled={lookupLoading} type="submit">
                    {lookupLoading ? "Locating…" : "Locate"}
                  </button>
                </div>
              </label>
              {lookupError ? <p className="lookup-error">{lookupError}</p> : null}
              {lookupResult ? (
                <p className="lookup-result">
                  key <span className="subtle-mono">{lookupResult.key}</span> resolved via{" "}
                  <strong>{lookupResult.encoding ?? "unknown"}</strong> into range{" "}
                  <strong>{lookupResult.range.range_id}</strong>
                </p>
              ) : null}
            </form>
            {!selectedRange ? <EmptyState label="Choose a range to inspect placement." /> : null}
            {selectedRange ? (
              <div className="placement-detail-stack">
                <dl className="metric-list">
                  <div>
                    <dt>keys</dt>
                    <dd className="subtle-mono">
                      {selectedRange.start_key || "∅"} .. {selectedRange.end_key || "∞"}
                    </dd>
                  </div>
                  <div>
                    <dt>leaseholder</dt>
                    <dd>{selectedRange.leaseholder_node_id ? `node ${selectedRange.leaseholder_node_id}` : "unknown"}</dd>
                  </div>
                  <div>
                    <dt>placement</dt>
                    <dd>{selectedRange.placement_mode ?? "unplaced"}</dd>
                  </div>
                  <div>
                    <dt>regions</dt>
                    <dd>{selectedRange.preferred_regions?.join(", ") || "none declared"}</dd>
                  </div>
                  <div>
                    <dt>lookup</dt>
                    <dd>{lookupResult?.range.range_id === selectedRange.range_id ? "selected by key lookup" : "manual selection"}</dd>
                  </div>
                </dl>

                <div className="placement-board">
                  {placementNodes.map(({ node, replica }) => {
                    const isLeaseholder = selectedRange.leaseholder_node_id === node.node_id;
                    return (
                      <article
                        className={`placement-node${replica ? " placement-node-hosting" : ""}${
                          isLeaseholder ? " placement-node-leaseholder" : ""
                        }`}
                        key={node.node_id}
                      >
                        <div className="placement-node-header">
                          <strong>node {node.node_id}</strong>
                          <StatusPill tone={node.status === "ok" ? "good" : "warn"} label={node.status} />
                        </div>
                        <p className="placement-role-copy">
                          {replica ? `${replica.role} replica present` : "no replica for this range"}
                        </p>
                        <div className="field-chip-row">
                          <span className="field-chip">{replica ? `replica ${replica.replica_id}` : "idle"}</span>
                          {isLeaseholder ? <span className="field-chip field-chip-leaseholder">leaseholder</span> : null}
                          {node.partitioned_from?.length ? (
                            <span className="field-chip">partitioned from {node.partitioned_from.join(", ")}</span>
                          ) : null}
                        </div>
                      </article>
                    );
                  })}
                </div>
              </div>
            ) : null}
        </div>
      </div>
    </section>
  );
}

function EventsPage(props: { events: ClusterEvent[] }) {
  const [filter, setFilter] = useState("");
  const deferredFilter = useDeferredValue(filter.trim().toLowerCase());
  const filtered = props.events
    .filter((event) => matchesEvent(event, deferredFilter))
    .slice()
    .reverse();

  return (
    <section className="page-grid">
      <div className="panel">
        <div className="panel-header panel-header-with-control">
          <div>
            <p className="eyebrow">Events</p>
            <h3>Live stream and replay window</h3>
          </div>
          <label className="filter-box">
            <span>Filter</span>
            <input
              aria-label="Filter events"
              onChange={(event) => setFilter(event.target.value)}
              placeholder="severity, type, node, message"
              value={filter}
            />
          </label>
        </div>
        <div className="event-stack">
          {filtered.length === 0 ? <EmptyState label="No events match the current filter." /> : null}
          {filtered.map((event) => (
            <EventRow event={event} key={event.id ?? `${event.timestamp}-${event.type}-${event.message}`} />
          ))}
        </div>
      </div>
    </section>
  );
}

function ScenariosPage() {
  const [runs, setRuns] = useState<ScenarioRunView[]>([]);
  const [selectedRunID, setSelectedRunID] = useState<string | null>(null);
  const [detail, setDetail] = useState<ScenarioRunDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadRuns = useEffectEvent(async () => {
    setLoading(true);
    try {
      const nextRuns = await fetchScenarioRuns();
      startTransition(() => {
        setRuns(nextRuns);
        setSelectedRunID((current) => current ?? nextRuns[0]?.run_id ?? null);
        setError(null);
        setLoading(false);
      });
    } catch (loadError) {
      const message = loadError instanceof Error ? loadError.message : "failed to load scenario runs";
      startTransition(() => {
        setError(message);
        setLoading(false);
      });
    }
  });

  const loadDetail = useEffectEvent(async (runID: string) => {
    try {
      const nextDetail = await fetchScenarioRun(runID);
      startTransition(() => {
        setDetail(nextDetail);
        setError(null);
      });
    } catch (loadError) {
      const message = loadError instanceof Error ? loadError.message : "failed to load scenario detail";
      startTransition(() => {
        setDetail(null);
        setError(message);
      });
    }
  });

  useEffect(() => {
    void loadRuns();
  }, []);

  useEffect(() => {
    if (!selectedRunID) {
      setDetail(null);
      return;
    }
    void loadDetail(selectedRunID);
  }, [selectedRunID]);

  return (
    <section className="page-grid">
      <div className="scenario-layout">
        <div className="panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Scenarios</p>
              <h3>Retained fault campaigns</h3>
            </div>
            <button className="refresh-button" type="button" onClick={() => void loadRuns()}>
              Refresh runs
            </button>
          </div>
          {loading ? <EmptyState label="Loading retained scenario runs…" /> : null}
          {!loading && error ? <p className="lookup-error">{error}</p> : null}
          {!loading && runs.length === 0 && !error ? <EmptyState label="No retained scenario artifacts configured." /> : null}
          <div className="scenario-list">
            {runs.map((run) => (
              <button
                className={`scenario-list-item${run.run_id === selectedRunID ? " scenario-list-item-active" : ""}`}
                key={run.run_id}
                onClick={() => setSelectedRunID(run.run_id)}
                type="button"
              >
                <div className="scenario-list-header">
                  <strong>{run.scenario_name}</strong>
                  <StatusPill tone={run.status === "pass" ? "good" : "bad"} label={run.status} />
                </div>
                <p>{formatInstant(run.finished_at)}</p>
                <span>
                  {run.step_count} steps, {run.node_log_count} node logs
                </span>
              </button>
            ))}
          </div>
        </div>

        <div className="panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Artifacts</p>
              <h3>Manifest, report, and node evidence</h3>
            </div>
            {detail ? <StatusPill tone={detail.run.status === "pass" ? "good" : "bad"} label={detail.run.run_id} /> : null}
          </div>
          {!detail ? <EmptyState label="Choose a retained run to inspect its artifacts." /> : null}
          {detail ? (
            <div className="scenario-detail-stack">
              <dl className="metric-list">
                <div>
                  <dt>status</dt>
                  <dd>{detail.summary.status}</dd>
                </div>
                <div>
                  <dt>steps</dt>
                  <dd>{detail.summary.step_count}</dd>
                </div>
                <div>
                  <dt>nodes</dt>
                  <dd>{detail.summary.node_count}</dd>
                </div>
                <div>
                  <dt>finished</dt>
                  <dd>{formatInstant(detail.summary.finished_at)}</dd>
                </div>
              </dl>

              {detail.summary.failure ? <p className="lookup-error">{detail.summary.failure}</p> : null}

              <div className="detail-section">
                <h4>Manifest steps</h4>
                <div className="event-stack">
                  {detail.manifest.steps.map((step) => (
                    <article className="event-row" key={`${detail.run.run_id}-${step.index}`}>
                      <div className="event-row-header">
                        <div>
                          <p className="event-type">
                            step {step.index}: {step.action}
                          </p>
                          <p className="event-copy">
                            {step.duration ? `duration ${step.duration}` : "fault action"}
                          </p>
                        </div>
                      </div>
                    </article>
                  ))}
                </div>
              </div>

              <div className="detail-section">
                <h4>Node logs</h4>
                <div className="event-stack">
                  {Object.entries(detail.node_logs).map(([nodeID, entries]) => (
                    <article className="event-row" key={nodeID}>
                      <div className="event-row-header">
                        <div>
                          <p className="event-type">node {nodeID}</p>
                          <p className="event-copy">{entries.length} retained entries</p>
                        </div>
                      </div>
                      <div className="field-chip-row">
                        {entries.slice(-4).map((entry) => (
                          <span className="field-chip" key={`${nodeID}-${entry.timestamp}-${entry.message}`}>
                            {formatInstant(entry.timestamp)} {entry.message}
                          </span>
                        ))}
                      </div>
                    </article>
                  ))}
                </div>
              </div>

              {detail.handoff ? (
                <div className="detail-section">
                  <h4>External handoff</h4>
                  <div className="field-chip-row">
                    {detail.handoff.operations.map((operation) => (
                      <span className="field-chip" key={operation.action}>
                        {operation.action} → {operation.external_operation}
                      </span>
                    ))}
                  </div>
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
      </div>
    </section>
  );
}

function MetricCard(props: { label: string; value: number; hint: string }) {
  return (
    <article className="metric-card">
      <p className="eyebrow">{props.label}</p>
      <strong>{props.value}</strong>
      <span>{props.hint}</span>
    </article>
  );
}

function StatusPill(props: { tone: "good" | "warn" | "bad" | "neutral"; label: string }) {
  return <span className={`status-pill status-pill-${props.tone}`}>{props.label}</span>;
}

function EventRow(props: { event: ClusterEvent }) {
  return (
    <article className="event-row">
      <div className="event-row-header">
        <div>
          <p className="event-type">{props.event.type}</p>
          <p className="event-copy">{props.event.message}</p>
        </div>
        <StatusPill tone={severityTone(props.event.severity)} label={props.event.severity ?? "info"} />
      </div>
      <div className="event-meta">
        <span>{formatInstant(props.event.timestamp)}</span>
        {props.event.node_id ? <span>node {props.event.node_id}</span> : null}
        {props.event.range_id ? <span>range {props.event.range_id}</span> : null}
        {props.event.id ? <span className="subtle-mono">{props.event.id.slice(0, 12)}</span> : null}
      </div>
      {props.event.fields && Object.keys(props.event.fields).length > 0 ? (
        <div className="field-chip-row">
          {Object.entries(props.event.fields).map(([key, value]) => (
            <span className="field-chip" key={key}>
              {key}={value}
            </span>
          ))}
        </div>
      ) : null}
    </article>
  );
}

function EmptyState(props: { label: string }) {
  return <p className="empty-state">{props.label}</p>;
}

function clusterHeadline(snapshot: ClusterSnapshot) {
  const degraded = snapshot.nodes.filter((node) => node.status !== "ok").length;
  if (degraded === 0) {
    return `${snapshot.nodes.length} nodes healthy, ${snapshot.ranges.length} authoritative ranges visible`;
  }
  return `${degraded} degraded node${degraded === 1 ? "" : "s"} across ${snapshot.nodes.length} visible nodes`;
}

function matchesRange(range: RangeView, filter: string) {
  if (!filter) {
    return true;
  }
  const haystack = [
    range.range_id,
    range.generation,
    range.start_key,
    range.end_key,
    range.leaseholder_node_id,
    range.placement_mode,
    range.source,
    range.preferred_regions?.join(" "),
    range.lease_preferences?.join(" "),
    range.replicas.map((replica) => `${replica.node_id} ${replica.role}`).join(" "),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return haystack.includes(filter);
}

function matchesEvent(event: ClusterEvent, filter: string) {
  if (!filter) {
    return true;
  }
  const haystack = [
    event.type,
    event.severity,
    event.message,
    event.node_id,
    event.range_id,
    event.id,
    event.timestamp,
    event.fields ? Object.entries(event.fields).map(([key, value]) => `${key} ${value}`).join(" ") : "",
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return haystack.includes(filter);
}

function placementNodesForRange(nodes: NodeView[], range: RangeView | null) {
  if (!range) {
    return [];
  }
  if (nodes.length === 0) {
    return range.replicas.map((replica) => ({
      node: {
        node_id: replica.node_id,
        status: "unknown",
        partitioned_from: [],
        notes: [],
        replica_count: 0,
        lease_count: 0,
      } as NodeView,
      replica,
    }));
  }
  return nodes
    .slice()
    .sort((left, right) => left.node_id - right.node_id)
    .map((node) => ({
      node,
      replica: range.replicas.find((replica) => replica.node_id === node.node_id) ?? null,
    }));
}

function severityTone(severity?: string): "good" | "warn" | "bad" | "neutral" {
  switch (severity) {
    case "error":
      return "bad";
    case "warning":
      return "warn";
    case "info":
      return "neutral";
    default:
      return "good";
  }
}

function formatInstant(value?: string | null) {
  if (!value) {
    return "n/a";
  }
  const instant = new Date(value);
  if (Number.isNaN(instant.getTime())) {
    return value;
  }
  return instant.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}
