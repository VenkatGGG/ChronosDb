import type {
  ClusterSnapshot,
  ClusterTopologyView,
  KeyLocationView,
  NodeDetailView,
  RangeDetailView,
  ScenarioRunDetail,
  ScenarioRunView,
} from "../types";

export async function fetchJSON<T>(input: RequestInfo | URL, init?: RequestInit): Promise<T> {
  const response = await fetch(input, init);
  if (!response.ok) {
    throw new Error(`request failed: ${response.status} ${response.statusText}`);
  }
  return (await response.json()) as T;
}

export function fetchClusterSnapshot(): Promise<ClusterSnapshot> {
  return fetchJSON<ClusterSnapshot>("/api/v1/cluster");
}

export function fetchTopology(): Promise<ClusterTopologyView> {
  return fetchJSON<ClusterTopologyView>("/api/v1/topology");
}

export function fetchKeyLocation(key: string): Promise<KeyLocationView> {
  const query = new URLSearchParams({ key });
  return fetchJSON<KeyLocationView>(`/api/v1/locate?${query.toString()}`);
}

export function fetchNodeDetail(nodeID: number): Promise<NodeDetailView> {
  return fetchJSON<NodeDetailView>(`/api/v1/nodes/${nodeID}`);
}

export function fetchRangeDetail(rangeID: number): Promise<RangeDetailView> {
  return fetchJSON<RangeDetailView>(`/api/v1/ranges/${rangeID}`);
}

export function fetchScenarioRuns(): Promise<ScenarioRunView[]> {
  return fetchJSON<ScenarioRunView[]>("/api/v1/scenarios");
}

export function fetchScenarioRun(runID: string): Promise<ScenarioRunDetail> {
  return fetchJSON<ScenarioRunDetail>(`/api/v1/scenarios/${encodeURIComponent(runID)}`);
}
