import { startTransition, useEffect, useEffectEvent, useState } from "react";
import { fetchClusterSnapshot } from "../lib/api";
import type { ClusterSnapshot } from "../types";

interface ClusterSnapshotState {
  snapshot: ClusterSnapshot | null;
  loading: boolean;
  error: string | null;
  lastUpdatedAt: string | null;
}

export function useClusterSnapshot(refreshMs: number) {
  const [state, setState] = useState<ClusterSnapshotState>({
    snapshot: null,
    loading: true,
    error: null,
    lastUpdatedAt: null,
  });

  const loadSnapshot = useEffectEvent(async (background: boolean) => {
    if (!background) {
      setState((current) => ({ ...current, loading: true, error: null }));
    }
    try {
      const snapshot = await fetchClusterSnapshot();
      startTransition(() => {
        setState({
          snapshot,
          loading: false,
          error: null,
          lastUpdatedAt: new Date().toISOString(),
        });
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown snapshot error";
      startTransition(() => {
        setState((current) => ({
          ...current,
          loading: false,
          error: message,
        }));
      });
    }
  });

  useEffect(() => {
    void loadSnapshot(false);
    const timer = window.setInterval(() => {
      void loadSnapshot(true);
    }, refreshMs);
    return () => {
      window.clearInterval(timer);
    };
  }, [refreshMs]);

  return {
    ...state,
    refresh: () => {
      void loadSnapshot(false);
    },
  };
}
