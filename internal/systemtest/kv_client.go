package systemtest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	chronosruntime "github.com/VenkatGGG/ChronosDb/internal/runtime"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/VenkatGGG/ChronosDb/internal/txn"
)

type kvClient struct {
	nodeID  uint64
	rootDir string
	host    *chronosruntime.Host
	client  *http.Client
}

type kvGetRequest struct {
	Key []byte `json:"key"`
}

type kvGetResponse struct {
	Found bool   `json:"found"`
	Value []byte `json:"value,omitempty"`
}

type kvPutRequest struct {
	Key       []byte        `json:"key"`
	Timestamp hlc.Timestamp `json:"timestamp"`
	Value     []byte        `json:"value"`
}

type kvScanRequest struct {
	StartKey       []byte `json:"start_key"`
	EndKey         []byte `json:"end_key"`
	StartInclusive bool   `json:"start_inclusive"`
	EndInclusive   bool   `json:"end_inclusive"`
}

type kvScanRow struct {
	LogicalKey []byte        `json:"logical_key"`
	Timestamp  hlc.Timestamp `json:"timestamp"`
	Value      []byte        `json:"value"`
}

type kvScanResponse struct {
	Rows []kvScanRow `json:"rows"`
}

func newKVClient(nodeID uint64, rootDir string, host *chronosruntime.Host) *kvClient {
	return &kvClient{
		nodeID:  nodeID,
		rootDir: rootDir,
		host:    host,
		client:  &http.Client{Timeout: 2 * time.Second},
	}
}

func (c *kvClient) GetLatest(ctx context.Context, key []byte) ([]byte, bool, error) {
	desc, err := c.host.LookupRange(ctx, key)
	if err != nil {
		return nil, false, err
	}
	targetNodeID, err := leaseholderNodeIDForRange(desc)
	if err != nil {
		return nil, false, err
	}
	if targetNodeID == c.nodeID {
		value, _, found, err := c.host.ReadLatestLocal(ctx, key)
		return value, found, err
	}
	var resp kvGetResponse
	if err := c.postJSON(ctx, targetNodeID, "/control/kv/get", kvGetRequest{Key: key}, &resp); err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

func (c *kvClient) Put(ctx context.Context, key, value []byte) error {
	return c.PutAt(ctx, key, hlc.Timestamp{WallTime: uint64(time.Now().UTC().UnixNano())}, value)
}

func (c *kvClient) PutAt(ctx context.Context, key []byte, ts hlc.Timestamp, value []byte) error {
	desc, err := c.host.LookupRange(ctx, key)
	if err != nil {
		return err
	}
	targetNodeID, err := leaseholderNodeIDForRange(desc)
	if err != nil {
		return err
	}
	if targetNodeID == c.nodeID {
		_, err := c.host.PutValueLocal(ctx, key, ts, value)
		return err
	}
	return c.postJSON(ctx, targetNodeID, "/control/kv/put", kvPutRequest{
		Key:       key,
		Timestamp: ts,
		Value:     value,
	}, nil)
}

func (c *kvClient) OnePhasePut(ctx context.Context, key, value []byte) error {
	desc, err := c.host.LookupRange(ctx, key)
	if err != nil {
		return err
	}
	now := hlc.Timestamp{WallTime: uint64(time.Now().UTC().UnixNano())}
	record := txn.Record{
		ID:       newTxnID(),
		Status:   txn.StatusPending,
		ReadTS:   now,
		WriteTS:  now,
		Priority: 1,
	}
	record, err = record.Anchor(desc.RangeID, now)
	if err != nil {
		return err
	}
	result, err := txn.OnePhaseCommit(txn.OnePhaseCommitRequest{
		Txn: record,
		Writes: []txn.PendingWrite{
			{
				Key:      append([]byte(nil), key...),
				RangeID:  desc.RangeID,
				Strength: storage.IntentStrengthExclusive,
				Value:    append([]byte(nil), value...),
			},
		},
	})
	if err != nil {
		return err
	}
	return c.PutAt(ctx, key, result.CommitTS, value)
}

func (c *kvClient) ScanRange(ctx context.Context, startKey, endKey []byte, startInclusive, endInclusive bool) ([]kvScanRow, error) {
	currentStart := append([]byte(nil), startKey...)
	currentStartInclusive := startInclusive
	rows := make([]kvScanRow, 0)
	for {
		desc, err := c.host.LookupRange(ctx, currentStart)
		if err != nil {
			return nil, err
		}
		targetNodeID, err := leaseholderNodeIDForRange(desc)
		if err != nil {
			return nil, err
		}
		segmentEnd, segmentEndInclusive, done := clampScanEnd(desc, endKey, endInclusive)
		request := kvScanRequest{
			StartKey:       currentStart,
			EndKey:         segmentEnd,
			StartInclusive: currentStartInclusive,
			EndInclusive:   segmentEndInclusive,
		}
		var segment kvScanResponse
		if targetNodeID == c.nodeID {
			localRows, _, err := c.host.ScanRangeLocal(ctx, request.StartKey, request.EndKey, request.StartInclusive, request.EndInclusive)
			if err != nil {
				return nil, err
			}
			segment.Rows = make([]kvScanRow, 0, len(localRows))
			for _, row := range localRows {
				segment.Rows = append(segment.Rows, kvScanRow{
					LogicalKey: row.LogicalKey,
					Timestamp:  row.Timestamp,
					Value:      row.Value,
				})
			}
		} else {
			if err := c.postJSON(ctx, targetNodeID, "/control/kv/scan", request, &segment); err != nil {
				return nil, err
			}
		}
		rows = append(rows, segment.Rows...)
		if done || len(desc.EndKey) == 0 {
			return rows, nil
		}
		currentStart = append([]byte(nil), desc.EndKey...)
		currentStartInclusive = true
	}
}

func (c *kvClient) postJSON(ctx context.Context, nodeID uint64, path string, reqBody any, respBody any) error {
	controlURL, err := resolveNodeControlURL(c.rootDir, nodeID)
	if err != nil {
		return err
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, controlURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("systemtest kv client: node %d %s returned status %d", nodeID, path, resp.StatusCode)
	}
	if respBody == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(respBody)
}

func newTxnID() storage.TxnID {
	var id storage.TxnID
	if _, err := rand.Read(id[:]); err == nil {
		return id
	}
	now := uint64(time.Now().UTC().UnixNano())
	binary.BigEndian.PutUint64(id[:8], now)
	return id
}

func leaseholderNodeIDForRange(desc meta.RangeDescriptor) (uint64, error) {
	for _, replica := range desc.Replicas {
		if replica.ReplicaID == desc.LeaseholderReplicaID {
			return replica.NodeID, nil
		}
	}
	return 0, fmt.Errorf("systemtest kv client: leaseholder replica %d not found for range %d", desc.LeaseholderReplicaID, desc.RangeID)
}

func clampScanEnd(desc meta.RangeDescriptor, requestedEnd []byte, requestedEndInclusive bool) ([]byte, bool, bool) {
	if len(desc.EndKey) == 0 {
		return append([]byte(nil), requestedEnd...), requestedEndInclusive, true
	}
	if len(requestedEnd) == 0 {
		return append([]byte(nil), desc.EndKey...), false, false
	}
	if bytes.Compare(requestedEnd, desc.EndKey) <= 0 {
		return append([]byte(nil), requestedEnd...), requestedEndInclusive, true
	}
	return append([]byte(nil), desc.EndKey...), false, false
}
