export interface ReplicaView {
  replica_id: number;
  node_id: number;
  role: string;
}

export interface RangeView {
  range_id: number;
  generation: number;
  start_key: string;
  end_key?: string;
  replicas: ReplicaView[];
  leaseholder_replica_id?: number;
  leaseholder_node_id?: number;
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

export interface ClusterSnapshot {
  generated_at: string;
  nodes: NodeView[];
  ranges: RangeView[];
  events?: ClusterEvent[];
}
