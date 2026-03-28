import { startTransition, useEffect, useEffectEvent, useState } from "react";
import type { ClusterEvent } from "../types";

interface EventStreamState {
  events: ClusterEvent[];
  status: "connecting" | "live" | "retrying";
  error: string | null;
}

export function useEventStream(seedEvents: ClusterEvent[], replay = 64, maxEvents = 256) {
  const [state, setState] = useState<EventStreamState>({
    events: mergeEvents([], seedEvents, maxEvents),
    status: "connecting",
    error: null,
  });

  const ingestEvents = useEffectEvent((incoming: ClusterEvent[]) => {
    if (incoming.length === 0) {
      return;
    }
    startTransition(() => {
      setState((current) => ({
        ...current,
        events: mergeEvents(current.events, incoming, maxEvents),
      }));
    });
  });

  useEffect(() => {
    ingestEvents(seedEvents);
  }, [seedEvents]);

  const setStreamStatus = useEffectEvent((status: EventStreamState["status"], error: string | null) => {
    startTransition(() => {
      setState((current) => ({
        ...current,
        status,
        error,
      }));
    });
  });

  useEffect(() => {
    const source = new EventSource(`/api/v1/events/stream?replay=${replay}`);
    const listener = (message: MessageEvent<string>) => {
      try {
        const payload = JSON.parse(message.data) as ClusterEvent;
        ingestEvents([payload]);
        setStreamStatus("live", null);
      } catch (error) {
        const reason = error instanceof Error ? error.message : "invalid event payload";
        setStreamStatus("retrying", reason);
      }
    };
    source.addEventListener("cluster_event", listener as EventListener);
    source.onopen = () => {
      setStreamStatus("live", null);
    };
    source.onerror = () => {
      setStreamStatus("retrying", "waiting for console event stream");
    };
    return () => {
      source.removeEventListener("cluster_event", listener as EventListener);
      source.close();
    };
  }, [replay]);

  return state;
}

function mergeEvents(current: ClusterEvent[], incoming: ClusterEvent[], maxEvents: number): ClusterEvent[] {
  const merged = [...current];
  const index = new Map<string, number>();
  for (let position = 0; position < merged.length; position += 1) {
    const event = merged[position];
    if (!event?.id) {
      continue;
    }
    index.set(event.id, position);
  }
  for (const event of incoming) {
    if (!event.id) {
      merged.push(event);
      continue;
    }
    const existing = index.get(event.id);
    if (existing !== undefined) {
      merged[existing] = event;
      continue;
    }
    index.set(event.id, merged.length);
    merged.push(event);
  }
  merged.sort((left, right) => left.timestamp.localeCompare(right.timestamp));
  if (merged.length <= maxEvents) {
    return merged;
  }
  return merged.slice(merged.length - maxEvents);
}
