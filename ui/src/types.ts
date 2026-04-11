export interface ReplicaView {
  replica_id: number;
  node_id: number;
  role: string;
}

export interface RangeTableStatView {
  table_id: number;
  table_name: string;
  row_count: number;
}

export interface RangeView {
  range_id: number;
  generation: number;
  start_key: string;
  end_key?: string;
  replicas: ReplicaView[];
  leaseholder_replica_id?: number;
  leaseholder_node_id?: number;
  keyspace?: string;
  shard_label?: string;
  row_count?: number;
  tables?: RangeTableStatView[];
  placement_mode?: string;
  preferred_regions?: string[];
  lease_preferences?: string[];
  source?: string;
}

export interface NodeView {
  node_id: number;
  pg_addr?: string;
  observability_url?: string;
  control_url?: string;
  status: string;
  started_at?: string;
  partitioned_from?: number[];
  notes?: string[];
  replica_count: number;
  lease_count: number;
}

export interface ClusterEvent {
  id?: string;
  timestamp: string;
  type: string;
  node_id?: number;
  range_id?: number;
  severity?: string;
  message: string;
  fields?: Record<string, string>;
}

export interface ClusterTableStatView {
  table_id: number;
  table_name: string;
  row_count: number;
  range_count: number;
}

export interface ClusterStatsView {
  total_rows: number;
  total_ranges: number;
  data_ranges: number;
  total_replicas: number;
  tables?: ClusterTableStatView[];
}

export interface ClusterSnapshot {
  generated_at: string;
  nodes: NodeView[];
  ranges: RangeView[];
  events?: ClusterEvent[];
  stats: ClusterStatsView;
}

export interface TopologyEdgeView {
  node_id: number;
  range_id: number;
  replica_id: number;
  role: string;
  leaseholder: boolean;
}

export interface ClusterTopologyView {
  generated_at: string;
  nodes: NodeView[];
  ranges: RangeView[];
  edges: TopologyEdgeView[];
  stats: ClusterStatsView;
}

export interface NodeHostedRangeView {
  range_id: number;
  generation: number;
  start_key: string;
  end_key?: string;
  replica_id: number;
  replica_role: string;
  leaseholder: boolean;
  keyspace?: string;
  shard_label?: string;
  row_count?: number;
  placement_mode?: string;
}

export interface NodeDetailView {
  node: NodeView;
  hosted_ranges: NodeHostedRangeView[];
  recent_events: ClusterEvent[];
}

export interface RangeReplicaNodeView {
  replica: ReplicaView;
  node?: NodeView;
  leaseholder: boolean;
}

export interface RangeDetailView {
  range: RangeView;
  replica_nodes: RangeReplicaNodeView[];
  recent_events: ClusterEvent[];
}

export interface KeyLocationView {
  key: string;
  encoding?: string;
  range: RangeView;
}

export interface ManifestStep {
  index: number;
  action: string;
  partition_left?: number[];
  partition_right?: number[];
  node_id?: number;
  duration?: string;
  gateway_node_id?: number;
  txn_label?: string;
  ack_delay?: string;
  drop_response?: boolean;
}

export interface Manifest {
  version: string;
  scenario: string;
  nodes: number[];
  steps: ManifestStep[];
}

export interface HandoffOperation {
  action: string;
  external_operation: string;
  description: string;
}

export interface HandoffBundle {
  version: string;
  manifest: Manifest;
  operations: HandoffOperation[];
}

export interface StepReport {
  index: number;
  action: string;
  started_at: string;
  finished_at: string;
  error?: string;
}

export interface RunReport {
  scenario_name: string;
  started_at: string;
  finished_at: string;
  steps: StepReport[];
}

export interface RunArtifactSummary {
  version: string;
  scenario_name: string;
  status: string;
  started_at: string;
  finished_at: string;
  step_count: number;
  failed_step?: number;
  failure?: string;
  node_count: number;
  node_log_count: number;
}

export interface NodeLogEntry {
  timestamp: string;
  message: string;
}

export interface ScenarioRunView {
  run_id: string;
  scenario_name: string;
  status: string;
  started_at: string;
  finished_at: string;
  step_count: number;
  failed_step?: number;
  failure?: string;
  node_count: number;
  node_log_count: number;
}

export interface ScenarioLiveCorrelation {
  generated_at: string;
  source: string;
  nodes: NodeView[];
  ranges: RangeView[];
  missing_node_ids?: number[];
}

export interface ScenarioRunDetail {
  run: ScenarioRunView;
  manifest: Manifest;
  handoff?: HandoffBundle;
  report: RunReport;
  summary: RunArtifactSummary;
  node_logs: Record<string, NodeLogEntry[]>;
  live_correlation?: ScenarioLiveCorrelation;
}
