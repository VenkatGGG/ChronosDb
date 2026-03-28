import type { ClusterSnapshot } from "../types";

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
