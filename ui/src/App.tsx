import { startTransition, useDeferredValue, useEffect, useEffectEvent, useState } from "react";
import { BrowserRouter, Link, NavLink, Navigate, Route, Routes, useParams } from "react-router-dom";
import { fetchKeyLocation, fetchNodeDetail, fetchRangeDetail, fetchScenarioRun, fetchScenarioRuns } from "./lib/api";
import { useClusterSnapshot } from "./hooks/useClusterSnapshot";
import { useEventStream } from "./hooks/useEventStream";
import type {
  ClusterEvent,
  ClusterSnapshot,
  KeyLocationView,
  NodeDetailView,
  NodeView,
  RangeDetailView,
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
            <p className="eyebrow">ChronosDB Console</p>
            <h1>Cluster Fabric</h1>
            <p className="rail-copy">
              Real-time shard layout, live row totals, and leaseholder placement across the cluster.
            </p>
          </div>
          <nav className="console-nav" aria-label="Primary">
            <NavItem to="/overview" label="Overview" subtitle="Rows, shards, and signal" />
            <NavItem to="/topology" label="Topology" subtitle="Replica fabric" />
            <NavItem to="/nodes" label="Nodes" subtitle="Health and hosting" />
            <NavItem to="/ranges" label="Ranges" subtitle="Shard inventory" />
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
              <p className="eyebrow">Live Cluster Surface</p>
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
            <Route
              path="/topology"
              element={
                <TopologyPage
                  loading={snapshotState.loading}
                  snapshot={snapshot}
                  events={streamState.events}
                  error={snapshotState.error}
                  onRefresh={snapshotState.refresh}
                />
              }
            />
            <Route path="/nodes" element={<NodesPage nodes={snapshot?.nodes ?? []} ranges={snapshot?.ranges ?? []} />} />
            <Route path="/nodes/:nodeId" element={<NodeDetailPage />} />
            <Route path="/ranges" element={<RangesPage nodes={snapshot?.nodes ?? []} ranges={snapshot?.ranges ?? []} />} />
            <Route path="/ranges/:rangeId" element={<RangeDetailPage />} />
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
  const recentEvents = props.events.slice(Math.max(props.events.length - 8, 0)).reverse();
  const tableStats = snapshot.stats.tables ?? [];

  return (
    <div className="page-grid">
      <section className="hero-panel">
        <div>
          <p className="eyebrow">Overview</p>
          <h3>Live rows, shards, and leaseholder flow</h3>
          <p className="hero-copy">
            Watch the cluster as a connected fabric instead of isolated cards. Each shard below maps replicas onto live
            nodes and shows its current logical row count.
          </p>
        </div>
        <div className="stats-grid">
          <MetricCard label="Logical rows" value={snapshot.stats.total_rows} hint={`${tableStats.length} live tables`} />
          <MetricCard label="Ranges" value={snapshot.stats.total_ranges} hint={`${snapshot.stats.data_ranges} data shards`} />
          <MetricCard label="Replicas" value={snapshot.stats.total_replicas} hint={`${degradedNodes.length} degraded nodes`} />
          <MetricCard label="Events" value={props.events.length} hint="retained stream window" />
        </div>
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Live shard fabric</p>
            <h3>Database shards connected to live nodes</h3>
          </div>
        </div>
        <div className="fabric-shell">
          <ClusterFabric nodes={snapshot.nodes} ranges={snapshot.ranges} />
        </div>
      </section>

      <section className="overview-secondary-grid">
        <div className="panel">
          <div>
            <p className="eyebrow">Tables</p>
            <h3>Logical data distribution</h3>
          </div>
          <div className="ledger-stack">
            {tableStats.length === 0 ? <EmptyState label="No table row stats available." /> : null}
            {tableStats.map((table) => (
              <article className="ledger-row" key={table.table_id || table.table_name}>
                <div>
                  <p className="ledger-title">{table.table_name}</p>
                  <p className="subtle-copy">
                    {table.range_count} shard{table.range_count === 1 ? "" : "s"}
                  </p>
                </div>
                <strong>{formatNumber(table.row_count)}</strong>
              </article>
            ))}
          </div>
        </div>

        <div className="panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Nodes</p>
              <h3>Replica and row residency</h3>
            </div>
          </div>
          <div className="node-summary-grid">
            {buildNodeResidency(snapshot.nodes, snapshot.ranges).map((entry) => (
              <article className="node-summary-card" key={entry.node.node_id}>
                <div className="node-summary-header">
                  <div>
                    <strong>node {entry.node.node_id}</strong>
                    <p className="subtle-copy">{entry.dataShards} data shards hosted</p>
                  </div>
                  <StatusPill tone={entry.node.status === "ok" ? "good" : "warn"} label={entry.node.status} />
                </div>
                <dl className="metric-list">
                  <div>
                    <dt>logical rows</dt>
                    <dd>{formatNumber(entry.hostedRows)}</dd>
                  </div>
                  <div>
                    <dt>replicas</dt>
                    <dd>{entry.node.replica_count}</dd>
                  </div>
                  <div>
                    <dt>leases</dt>
                    <dd>{entry.node.lease_count}</dd>
                  </div>
                  <div>
                    <dt>network</dt>
                    <dd>{entry.node.partitioned_from?.length ? `isolated from ${entry.node.partitioned_from.join(", ")}` : "healthy"}</dd>
                  </div>
                </dl>
              </article>
            ))}
          </div>
        </div>

        <div className="panel">
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
        </div>
      </section>
    </div>
  );
}

function NodesPage(props: { nodes: NodeView[]; ranges: RangeView[] }) {
  const residency = buildNodeResidency(props.nodes, props.ranges);
  return (
    <section className="page-grid">
      <div className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Nodes</p>
            <h3>Health, endpoints, and hosting posture</h3>
          </div>
        </div>
        <div className="node-card-grid">
          {props.nodes.length === 0 ? <EmptyState label="No nodes surfaced by the console API." /> : null}
          {residency.map(({ node, hostedRows, dataShards }) => (
            <article className="node-card" key={node.node_id}>
              <div className="node-card-header">
                <div>
                  <p className="node-id">
                    <Link className="detail-link" to={`/nodes/${node.node_id}`}>
                      node {node.node_id}
                    </Link>
                  </p>
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
                  <dt>hosted rows</dt>
                  <dd>{formatNumber(hostedRows)}</dd>
                </div>
                <div>
                  <dt>data shards</dt>
                  <dd>{dataShards}</dd>
                </div>
                <div>
                  <dt>network</dt>
                  <dd>{node.partitioned_from?.length ? `isolated from ${node.partitioned_from.join(", ")}` : "healthy"}</dd>
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
              <h3>Shard inventory, replica spread, and live row counts</h3>
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
                    <th>shard</th>
                    <th>keys</th>
                    <th>rows</th>
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
                        <strong>
                          <Link className="detail-link" to={`/ranges/${range.range_id}`}>
                            {range.range_id}
                          </Link>
                        </strong>
                        <span className="subtle-mono">gen {range.generation}</span>
                      </td>
                      <td>
                        <strong>{range.shard_label ?? `${range.keyspace ?? "range"} shard`}</strong>
                        <span className="subtle-copy">
                          {range.tables?.map((table) => table.table_name).join(", ") || range.keyspace || "unclassified"}
                        </span>
                      </td>
                      <td className="subtle-mono">
                        {range.start_key || "∅"} .. {range.end_key || "∞"}
                      </td>
                      <td>{formatNumber(range.row_count ?? 0)}</td>
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
                    <dt>shard</dt>
                    <dd>{selectedRange.shard_label ?? "range shard"}</dd>
                  </div>
                  <div>
                    <dt>keys</dt>
                    <dd className="subtle-mono">
                      {selectedRange.start_key || "∅"} .. {selectedRange.end_key || "∞"}
                    </dd>
                  </div>
                  <div>
                    <dt>rows</dt>
                    <dd>{formatNumber(selectedRange.row_count ?? 0)}</dd>
                  </div>
                  <div>
                    <dt>leaseholder</dt>
                    <dd>{selectedRange.leaseholder_node_id ? `node ${selectedRange.leaseholder_node_id}` : "unknown"}</dd>
                  </div>
                  <div>
                    <dt>tables</dt>
                    <dd>{selectedRange.tables?.map((table) => table.table_name).join(", ") || "none"}</dd>
                  </div>
                  <div>
                    <dt>placement</dt>
                    <dd>{selectedRange.placement_mode ?? "unplaced"}</dd>
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

export function TopologyPage(props: {
  snapshot?: ClusterSnapshot | null;
  events?: ClusterEvent[];
  loading?: boolean;
  error?: string | null;
  onRefresh?: () => void;
}) {
  const snapshot = props.snapshot ?? null;
  const events = props.events ?? [];
  const hottestTables = (snapshot?.stats.tables ?? []).slice(0, 5);
  const highlightedRanges = (snapshot?.ranges ?? [])
    .slice()
    .sort((left, right) => {
      const rowDelta = (right.row_count ?? 0) - (left.row_count ?? 0);
      if (rowDelta !== 0) {
        return rowDelta;
      }
      return left.range_id - right.range_id;
    })
    .slice(0, 6);
  return (
    <section className="page-grid">
      <div className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Topology</p>
            <h3>Connected shard fabric and residency map</h3>
          </div>
          <button className="refresh-button" onClick={() => props.onRefresh?.()} type="button">
            Refresh
          </button>
        </div>
        {props.loading && !snapshot ? <EmptyState label="Loading topology graph…" /> : null}
        {!props.loading && props.error ? <p className="lookup-error">{props.error}</p> : null}
        {snapshot ? (
          <div className="page-grid">
            <div className="stats-grid">
              <MetricCard label="Nodes" value={snapshot.nodes.length} hint="live cluster members" />
              <MetricCard label="Rows" value={snapshot.stats.total_rows} hint="logical primary rows" />
              <MetricCard label="Ranges" value={snapshot.stats.total_ranges} hint={`${snapshot.stats.data_ranges} data-facing shards`} />
              <MetricCard label="Replicas" value={snapshot.stats.total_replicas} hint="total residency slots" />
            </div>

            <div className="panel topology-hero-panel">
              <ClusterFabric nodes={snapshot.nodes} ranges={snapshot.ranges} />
            </div>

            <div className="topology-secondary-grid">
              <div className="panel">
                <div className="panel-header">
                  <div>
                    <p className="eyebrow">Tables</p>
                    <h3>Row density by table</h3>
                  </div>
                </div>
                <div className="ledger-stack">
                  {hottestTables.length === 0 ? <EmptyState label="No table stats available." /> : null}
                  {hottestTables.map((table) => (
                    <article className="ledger-row" key={table.table_id || table.table_name}>
                      <div>
                        <p className="ledger-title">{table.table_name}</p>
                        <p className="subtle-copy">{table.range_count} shards</p>
                      </div>
                      <strong>{formatNumber(table.row_count)}</strong>
                    </article>
                  ))}
                </div>
              </div>

              <div className="panel">
                <div className="panel-header">
                  <div>
                    <p className="eyebrow">Hot shards</p>
                    <h3>Largest row-bearing ranges</h3>
                  </div>
                </div>
                <div className="ledger-stack">
                  {highlightedRanges.length === 0 ? <EmptyState label="No ranges available." /> : null}
                  {highlightedRanges.map((range) => (
                    <Link className="ledger-row ledger-row-link" key={range.range_id} to={`/ranges/${range.range_id}`}>
                      <div>
                        <p className="ledger-title">{range.shard_label ?? `range ${range.range_id}`}</p>
                        <p className="subtle-copy">
                          range {range.range_id} · leaseholder node {range.leaseholder_node_id ?? "?"}
                        </p>
                      </div>
                      <strong>{formatNumber(range.row_count ?? 0)}</strong>
                    </Link>
                  ))}
                </div>
              </div>
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <p className="eyebrow">Recent topology events</p>
                  <h3>Movements, splits, and lease changes</h3>
                </div>
              </div>
              <div className="event-stack">
                {events.length === 0 ? <EmptyState label="No topology events received." /> : null}
                {events.slice(Math.max(events.length - 8, 0)).reverse().map((event) => (
                  <EventRow event={event} key={event.id ?? `${event.timestamp}-${event.type}-${event.message}`} />
                ))}
              </div>
            </div>
          </div>
        ) : null}
      </div>
    </section>
  );
}

export function NodeDetailPage() {
  const params = useParams();
  const nodeID = Number(params.nodeId);
  const [detail, setDetail] = useState<NodeDetailView | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadDetail = useEffectEvent(async (nextNodeID: number) => {
    setLoading(true);
    try {
      const nextDetail = await fetchNodeDetail(nextNodeID);
      startTransition(() => {
        setDetail(nextDetail);
        setError(null);
        setLoading(false);
      });
    } catch (loadError) {
      const message = loadError instanceof Error ? loadError.message : "failed to load node detail";
      startTransition(() => {
        setDetail(null);
        setError(message);
        setLoading(false);
      });
    }
  });

  useEffect(() => {
    if (!Number.isFinite(nodeID) || nodeID <= 0) {
      setError("invalid node id");
      setLoading(false);
      return;
    }
    void loadDetail(nodeID);
  }, [nodeID]);

  return (
    <section className="page-grid">
      <div className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Node drilldown</p>
            <h3>{detail ? `node ${detail.node.node_id}` : "Node detail"}</h3>
          </div>
          <Link className="detail-link" to="/nodes">
            Back to nodes
          </Link>
        </div>
        {loading ? <EmptyState label="Loading node detail…" /> : null}
        {!loading && error ? <p className="lookup-error">{error}</p> : null}
        {detail ? (
          <div className="detail-grid">
            <article className="detail-section">
              <h4>Node surface</h4>
              <dl className="metric-list">
                <div>
                  <dt>status</dt>
                  <dd>{detail.node.status}</dd>
                </div>
                <div>
                  <dt>replicas</dt>
                  <dd>{detail.node.replica_count}</dd>
                </div>
                <div>
                  <dt>leases</dt>
                  <dd>{detail.node.lease_count}</dd>
                </div>
                <div>
                  <dt>started</dt>
                  <dd>{formatInstant(detail.node.started_at)}</dd>
                </div>
              </dl>
            </article>
            <article className="detail-section">
              <h4>Hosted ranges</h4>
              <div className="field-chip-row">
                {detail.hosted_ranges.map((range) => (
                  <Link className="field-chip field-chip-link" key={`${range.range_id}-${range.replica_id}`} to={`/ranges/${range.range_id}`}>
                    r{range.range_id} {range.shard_label ?? range.replica_role}
                    {range.row_count ? ` · ${formatNumber(range.row_count)} rows` : ""}
                    {range.leaseholder ? " leaseholder" : ""}
                  </Link>
                ))}
              </div>
            </article>
            <article className="detail-section">
              <h4>Recent related events</h4>
              <div className="event-stack">
                {detail.recent_events.length === 0 ? <EmptyState label="No related events recorded." /> : null}
                {detail.recent_events.map((event) => (
                  <EventRow event={event} key={event.id ?? `${event.timestamp}-${event.type}-${event.message}`} />
                ))}
              </div>
            </article>
          </div>
        ) : null}
      </div>
    </section>
  );
}

export function RangeDetailPage() {
  const params = useParams();
  const rangeID = Number(params.rangeId);
  const [detail, setDetail] = useState<RangeDetailView | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadDetail = useEffectEvent(async (nextRangeID: number) => {
    setLoading(true);
    try {
      const nextDetail = await fetchRangeDetail(nextRangeID);
      startTransition(() => {
        setDetail(nextDetail);
        setError(null);
        setLoading(false);
      });
    } catch (loadError) {
      const message = loadError instanceof Error ? loadError.message : "failed to load range detail";
      startTransition(() => {
        setDetail(null);
        setError(message);
        setLoading(false);
      });
    }
  });

  useEffect(() => {
    if (!Number.isFinite(rangeID) || rangeID <= 0) {
      setError("invalid range id");
      setLoading(false);
      return;
    }
    void loadDetail(rangeID);
  }, [rangeID]);

  return (
    <section className="page-grid">
      <div className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Range drilldown</p>
            <h3>{detail ? `range ${detail.range.range_id}` : "Range detail"}</h3>
          </div>
          <Link className="detail-link" to="/ranges">
            Back to ranges
          </Link>
        </div>
        {loading ? <EmptyState label="Loading range detail…" /> : null}
        {!loading && error ? <p className="lookup-error">{error}</p> : null}
        {detail ? (
          <div className="detail-grid">
            <article className="detail-section">
              <h4>Descriptor</h4>
              <dl className="metric-list">
                <div>
                  <dt>shard</dt>
                  <dd>{detail.range.shard_label ?? "range shard"}</dd>
                </div>
                <div>
                  <dt>generation</dt>
                  <dd>{detail.range.generation}</dd>
                </div>
                <div>
                  <dt>rows</dt>
                  <dd>{formatNumber(detail.range.row_count ?? 0)}</dd>
                </div>
                <div>
                  <dt>keys</dt>
                  <dd className="subtle-mono">
                    {detail.range.start_key || "∅"} .. {detail.range.end_key || "∞"}
                  </dd>
                </div>
                <div>
                  <dt>leaseholder</dt>
                  <dd>{detail.range.leaseholder_node_id ? `node ${detail.range.leaseholder_node_id}` : "unknown"}</dd>
                </div>
                <div>
                  <dt>placement</dt>
                  <dd>{detail.range.placement_mode ?? "unplaced"}</dd>
                </div>
              </dl>
            </article>
            <article className="detail-section">
              <h4>Replica residency</h4>
              <div className="placement-board">
                {detail.replica_nodes.map((entry) => (
                  <article
                    className={`placement-node placement-node-hosting${entry.leaseholder ? " placement-node-leaseholder" : ""}`}
                    key={entry.replica.replica_id}
                  >
                    <div className="placement-node-header">
                      <strong>
                        {entry.node ? <Link className="detail-link" to={`/nodes/${entry.node.node_id}`}>node {entry.node.node_id}</Link> : `node ${entry.replica.node_id}`}
                      </strong>
                      <StatusPill tone={entry.node?.status === "ok" ? "good" : "warn"} label={entry.replica.role} />
                    </div>
                    <p className="placement-role-copy">
                      replica {entry.replica.replica_id}
                      {entry.leaseholder ? ", current leaseholder" : ""}
                    </p>
                  </article>
                ))}
              </div>
            </article>
            <article className="detail-section">
              <h4>Recent range events</h4>
              <div className="event-stack">
                {detail.recent_events.length === 0 ? <EmptyState label="No range-scoped events recorded." /> : null}
                {detail.recent_events.map((event) => (
                  <EventRow event={event} key={event.id ?? `${event.timestamp}-${event.type}-${event.message}`} />
                ))}
              </div>
            </article>
          </div>
        ) : null}
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

export function ScenariosPage() {
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
                      <div className="field-chip-row">
                        {step.node_id ? (
                          <Link className="field-chip field-chip-link" to={`/nodes/${step.node_id}`}>
                            node {step.node_id}
                          </Link>
                        ) : null}
                        {step.gateway_node_id ? (
                          <Link className="field-chip field-chip-link" to={`/nodes/${step.gateway_node_id}`}>
                            gateway node {step.gateway_node_id}
                          </Link>
                        ) : null}
                      </div>
                    </article>
                  ))}
                </div>
              </div>

              {detail.live_correlation ? (
                <div className="detail-section">
                  <h4>Live topology correlation</h4>
                  <p className="subtle-copy">
                    Derived from current authoritative topology for nodes named in the retained manifest.
                  </p>
                  <div className="field-chip-row">
                    {detail.live_correlation.nodes.map((node) => (
                      <Link className="field-chip field-chip-link" key={`scenario-node-${node.node_id}`} to={`/nodes/${node.node_id}`}>
                        node {node.node_id}
                      </Link>
                    ))}
                    {detail.live_correlation.missing_node_ids?.map((nodeID) => (
                      <span className="field-chip" key={`missing-node-${nodeID}`}>
                        node {nodeID} missing
                      </span>
                    ))}
                  </div>
                  <div className="field-chip-row">
                    {detail.live_correlation.ranges.map((range) => (
                      <Link className="field-chip field-chip-link" key={`scenario-range-${range.range_id}`} to={`/ranges/${range.range_id}`}>
                        range {range.range_id}
                      </Link>
                    ))}
                    {detail.live_correlation.ranges.length === 0 ? (
                      <span className="field-chip">no live ranges correlated</span>
                    ) : null}
                  </div>
                </div>
              ) : null}

              <div className="detail-section">
                <h4>Node logs</h4>
                <div className="event-stack">
                  {Object.entries(detail.node_logs).map(([nodeID, entries]) => (
                    <article className="event-row" key={nodeID}>
                      <div className="event-row-header">
                        <div>
                          <p className="event-type">
                            <Link className="detail-link" to={`/nodes/${nodeID}`}>
                              node {nodeID}
                            </Link>
                          </p>
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

function ClusterFabric(props: { nodes: NodeView[]; ranges: RangeView[] }) {
  const sortedNodes = props.nodes.slice().sort((left, right) => left.node_id - right.node_id);
  const sortedRanges = props.ranges
    .slice()
    .sort((left, right) => {
      const leftRows = left.row_count ?? 0;
      const rightRows = right.row_count ?? 0;
      if (leftRows !== rightRows) {
        return rightRows - leftRows;
      }
      return left.range_id - right.range_id;
    });

  if (sortedNodes.length === 0) {
    return <EmptyState label="No nodes available for the shard fabric." />;
  }

  return (
    <div className="fabric-board">
      <div className="fabric-node-strip" style={{ gridTemplateColumns: `repeat(${sortedNodes.length}, minmax(0, 1fr))` }}>
        {sortedNodes.map((node) => (
          <article className="fabric-node-card" key={node.node_id}>
            <div className="fabric-node-card-header">
              <strong>
                <Link className="detail-link" to={`/nodes/${node.node_id}`}>
                  node {node.node_id}
                </Link>
              </strong>
              <StatusPill tone={node.status === "ok" ? "good" : "warn"} label={node.status} />
            </div>
            <dl className="fabric-node-metrics">
              <div>
                <dt>replicas</dt>
                <dd>{node.replica_count}</dd>
              </div>
              <div>
                <dt>leases</dt>
                <dd>{node.lease_count}</dd>
              </div>
            </dl>
          </article>
        ))}
      </div>

      <div className="fabric-range-stack">
        {sortedRanges.map((range) => (
          <article className="fabric-range-row" key={range.range_id}>
            <div className="fabric-range-header">
              <div>
                <p className="fabric-range-title">
                  <Link className="detail-link" to={`/ranges/${range.range_id}`}>
                    {range.shard_label ?? `range ${range.range_id}`}
                  </Link>
                </p>
                <p className="subtle-copy">
                  range {range.range_id} · {range.tables?.map((table) => table.table_name).join(", ") || range.keyspace || "range"} ·{" "}
                  {formatNumber(range.row_count ?? 0)} rows
                </p>
              </div>
              <div className="field-chip-row">
                <span className="field-chip">gen {range.generation}</span>
                <span className="field-chip">
                  leaseholder node {range.leaseholder_node_id ?? "?"}
                </span>
              </div>
            </div>

            <div className="fabric-lane-grid" style={{ gridTemplateColumns: `repeat(${sortedNodes.length}, minmax(0, 1fr))` }}>
              {sortedNodes.map((node) => {
                const replica = range.replicas.find((entry) => entry.node_id === node.node_id);
                const leaseholder = range.leaseholder_node_id === node.node_id;
                return (
                  <div
                    className={`fabric-lane-cell${replica ? " fabric-lane-cell-hosting" : ""}${leaseholder ? " fabric-lane-cell-leaseholder" : ""}`}
                    key={`${range.range_id}-${node.node_id}`}
                  >
                    <span className="fabric-lane-rail" />
                    <span className="fabric-lane-dot" />
                    <span className="fabric-lane-caption">
                      {replica ? `${replica.role}${leaseholder ? " · leaseholder" : ""}` : "no replica"}
                    </span>
                  </div>
                );
              })}
            </div>
          </article>
        ))}
      </div>
    </div>
  );
}

function MetricCard(props: { label: string; value: number; hint: string }) {
  return (
    <article className="metric-card">
      <p className="eyebrow">{props.label}</p>
      <strong>{formatNumber(props.value)}</strong>
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
    return `${snapshot.nodes.length} nodes healthy, ${formatNumber(snapshot.stats.total_rows)} rows across ${snapshot.stats.total_ranges} live ranges`;
  }
  return `${degraded} degraded node${degraded === 1 ? "" : "s"} while ${formatNumber(snapshot.stats.total_rows)} rows remain visible`;
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
    range.keyspace,
    range.shard_label,
    range.row_count,
    range.placement_mode,
    range.source,
    range.tables?.map((table) => `${table.table_name} ${table.row_count}`).join(" "),
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

function buildNodeResidency(nodes: NodeView[], ranges: RangeView[]) {
  return nodes
    .slice()
    .sort((left, right) => left.node_id - right.node_id)
    .map((node) => {
      let hostedRows = 0;
      let dataShards = 0;
      for (const range of ranges) {
        if (range.replicas.some((replica) => replica.node_id === node.node_id)) {
          hostedRows += range.row_count ?? 0;
          if (range.tables?.length) {
            dataShards++
          }
        }
      }
      return { node, hostedRows, dataShards };
    });
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

function formatNumber(value: number) {
  return new Intl.NumberFormat().format(value);
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
