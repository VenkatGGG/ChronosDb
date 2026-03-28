import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { NodeDetailPage, RangeDetailPage, ScenariosPage } from "./App";

function jsonResponse(payload: unknown) {
  return new Response(JSON.stringify(payload), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

describe("console drilldowns", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/api/v1/nodes/1")) {
        return jsonResponse({
          node: {
            node_id: 1,
            status: "ok",
            replica_count: 2,
            lease_count: 1,
          },
          hosted_ranges: [
            {
              range_id: 11,
              generation: 4,
              start_key: "61",
              end_key: "6d",
              replica_id: 1,
              replica_role: "voter",
              leaseholder: true,
              placement_mode: "REGIONAL",
            },
          ],
          recent_events: [],
        });
      }
      if (url.endsWith("/api/v1/ranges/11")) {
        return jsonResponse({
          range: {
            range_id: 11,
            generation: 4,
            start_key: "61",
            end_key: "6d",
            replicas: [{ replica_id: 1, node_id: 1, role: "voter" }],
            leaseholder_replica_id: 1,
            leaseholder_node_id: 1,
            placement_mode: "REGIONAL",
          },
          replica_nodes: [
            {
              replica: { replica_id: 1, node_id: 1, role: "voter" },
              node: {
                node_id: 1,
                status: "ok",
                replica_count: 2,
                lease_count: 1,
              },
              leaseholder: true,
            },
          ],
          recent_events: [],
        });
      }
      if (url.endsWith("/api/v1/scenarios")) {
        return jsonResponse([
          {
            run_id: "minority-partition",
            scenario_name: "minority-partition",
            status: "pass",
            started_at: "2026-03-28T00:00:00Z",
            finished_at: "2026-03-28T00:01:00Z",
            step_count: 1,
            node_count: 3,
            node_log_count: 1,
          },
        ]);
      }
      if (url.endsWith("/api/v1/scenarios/minority-partition")) {
        return jsonResponse({
          run: {
            run_id: "minority-partition",
            scenario_name: "minority-partition",
            status: "pass",
            started_at: "2026-03-28T00:00:00Z",
            finished_at: "2026-03-28T00:01:00Z",
            step_count: 1,
            node_count: 3,
            node_log_count: 1,
          },
          manifest: {
            version: "chronosdb.systemtest.v1",
            scenario: "minority-partition",
            nodes: [1, 2, 3],
            steps: [{ index: 1, action: "crash_node", node_id: 1 }],
          },
          report: {
            scenario_name: "minority-partition",
            started_at: "2026-03-28T00:00:00Z",
            finished_at: "2026-03-28T00:01:00Z",
            steps: [],
          },
          summary: {
            version: "chronosdb.systemtest.artifacts.v1",
            scenario_name: "minority-partition",
            status: "pass",
            started_at: "2026-03-28T00:00:00Z",
            finished_at: "2026-03-28T00:01:00Z",
            step_count: 1,
            node_count: 3,
            node_log_count: 1,
          },
          node_logs: {
            "1": [{ timestamp: "2026-03-28T00:00:10Z", message: "node up" }],
          },
          live_correlation: {
            generated_at: "2026-03-28T00:01:30Z",
            source: "manifest_nodes_current_topology",
            nodes: [
              { node_id: 1, status: "ok", replica_count: 2, lease_count: 1 },
              { node_id: 2, status: "ok", replica_count: 1, lease_count: 0 },
            ],
            ranges: [
              {
                range_id: 11,
                generation: 4,
                start_key: "61",
                end_key: "6d",
                replicas: [{ replica_id: 1, node_id: 1, role: "voter" }],
                leaseholder_replica_id: 1,
                leaseholder_node_id: 1,
              },
            ],
          },
        });
      }
      throw new Error(`unexpected fetch ${url}`);
    }));
  });

  it("renders node and range detail routes from deep-link paths", async () => {
    render(
      <MemoryRouter initialEntries={["/nodes/1"]}>
        <Routes>
          <Route path="/nodes/:nodeId" element={<NodeDetailPage />} />
        </Routes>
      </MemoryRouter>,
    );

    expect(await screen.findByText("Node surface")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /r11 voter leaseholder/i })).toHaveAttribute("href", "/ranges/11");

    render(
      <MemoryRouter initialEntries={["/ranges/11"]}>
        <Routes>
          <Route path="/ranges/:rangeId" element={<RangeDetailPage />} />
        </Routes>
      </MemoryRouter>,
    );

    expect(await screen.findByText("Replica residency")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /node 1/i })).toHaveAttribute("href", "/nodes/1");
  });

  it("links scenario artifacts to live nodes and ranges", async () => {
    render(
      <MemoryRouter>
        <ScenariosPage />
      </MemoryRouter>,
    );

    expect(await screen.findByText("Live topology correlation")).toBeInTheDocument();
    const nodeLinks = screen.getAllByRole("link", { name: /^node 1$/i });
    expect(nodeLinks[0]).toHaveAttribute("href", "/nodes/1");
    expect(screen.getByRole("link", { name: /^range 11$/i })).toHaveAttribute("href", "/ranges/11");
  });
});
