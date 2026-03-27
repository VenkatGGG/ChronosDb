# Placement and Survival

## Purpose

Define the user-visible placement modes and the internal survival-goal rules that
drive replica counts and lease preferences.

## Placement Modes

### `REGIONAL`

Data, quorum, and lease preference stay in one region.

Default layout:

- 3 replicas
- spread across 3 availability zones when possible
- survival goal: zone failure

### `HOME_REGION`

Data spans multiple regions, but one region is the write home and default
leaseholder location.

Default layout:

- 5 replicas across at least 3 regions
- lease preference pinned to the home region
- survival goal: region failure

### `GLOBAL`

Data is explicitly multi-region and latency tradeoffs are accepted.

Default layout:

- 5 replicas across at least 3 regions
- lease preferences may be policy-driven rather than pinned
- survival goal: region failure unless explicitly reduced

## Survival Goals

### `ZONE_FAILURE`

The range remains available after one availability-zone failure.

Default requirement:

- 3 replicas in one region across distinct zones

### `REGION_FAILURE`

The range remains available after one full-region failure.

Default requirement:

- 5 replicas across at least 3 regions

## Descriptor-Level Policy Fields

Each placement-aware descriptor must eventually define:

- `placement_mode`
- `home_region` if applicable
- `survival_goal`
- `preferred_regions`
- `lease_preferences`
- `replica_constraints`

## SQL-Facing Abstraction

ChronosDB should expose placement in SQL terms, not raw replica math.

Examples of the user model:

- table is `REGIONAL` in `us-east1`
- table is `HOME_REGION` with home `us-east1`
- table is `GLOBAL` with region-failure survival

The SQL surface must compile these declarations into descriptor-level policy.

## Allocator Rules

- allocator must obey survival goal first
- allocator must obey explicit replica constraints next
- lease placement should follow lease preference policy
- hotspot mitigation may move leaseholders, but not in violation of declared placement

## Transaction and Routing Implications

- leaseholder routing must prefer the placement-aware leaseholder
- multi-region write latency is a function of survival goal and placement
- follower historical reads may be local if closed timestamp rules allow

## Defaults

- default database mode: `REGIONAL`
- default regional survival goal: `ZONE_FAILURE`
- default multi-region survival goal: `REGION_FAILURE`

## Tests Required

- allocator respects zone-failure regional layout
- allocator respects region-failure multi-region layout
- leaseholder preferences follow placement policy
- invalid SQL placement declarations map to `Client/Semantic` errors
