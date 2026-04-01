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

type kvDeleteRequest struct {
	Key       []byte        `json:"key"`
	Timestamp hlc.Timestamp `json:"timestamp"`
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

type kvIntentRequest struct {
	Key    []byte         `json:"key"`
	Intent storage.Intent `json:"intent"`
}

type kvIntentResponse struct {
	Found  bool           `json:"found"`
	Intent storage.Intent `json:"intent"`
}

type kvTxnRecordRequest struct {
	Record txn.Record `json:"record"`
}

type kvTxnRecordResponse struct {
	Found  bool       `json:"found"`
	Record txn.Record `json:"record"`
}

type kvTxnRecordScanRequest struct {
	StartKey []byte `json:"start_key"`
	EndKey   []byte `json:"end_key"`
}

type kvTxnRecordScanResponse struct {
	Records []txn.Record `json:"records"`
}

type kvLockAcquireRequest struct {
	Key      []byte                 `json:"key"`
	Txn      txn.Record             `json:"txn"`
	Strength storage.IntentStrength `json:"strength"`
}

type kvLockAcquireResponse struct {
	Decision txn.LockDecision `json:"decision"`
}

type kvLockReleaseRequest struct {
	Key   []byte        `json:"key"`
	TxnID storage.TxnID `json:"txn_id"`
}

type rangeStatusRequest struct {
	RangeID uint64 `json:"range_id"`
}

type rangePrepareRequest struct {
	RangeID    uint64                         `json:"range_id"`
	ReplicaID  uint64                         `json:"replica_id"`
	Descriptor meta.RangeDescriptor           `json:"descriptor"`
	Snapshot   chronosruntime.ReplicaSnapshot `json:"snapshot"`
}

type rangeStatusResponse struct {
	Hosted           bool                 `json:"hosted"`
	LocalReplicaID   uint64               `json:"local_replica_id,omitempty"`
	LeaderReplicaID  uint64               `json:"leader_replica_id,omitempty"`
	AppliedIndex     uint64               `json:"applied_index,omitempty"`
	Descriptor       meta.RangeDescriptor `json:"descriptor,omitempty"`
	DescriptorSource string               `json:"descriptor_source,omitempty"`
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

func (c *kvClient) LookupRange(ctx context.Context, key []byte) (meta.RangeDescriptor, error) {
	return c.host.LookupRange(ctx, key)
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

func (c *kvClient) DeleteAt(ctx context.Context, key []byte, ts hlc.Timestamp) error {
	desc, err := c.host.LookupRange(ctx, key)
	if err != nil {
		return err
	}
	targetNodeID, err := leaseholderNodeIDForRange(desc)
	if err != nil {
		return err
	}
	if targetNodeID == c.nodeID {
		_, err := c.host.DeleteValueLocal(ctx, key, ts)
		return err
	}
	return c.postJSON(ctx, targetNodeID, "/control/kv/delete", kvDeleteRequest{
		Key:       key,
		Timestamp: ts,
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

func (c *kvClient) PutIntent(ctx context.Context, key []byte, intent storage.Intent) error {
	desc, err := c.host.LookupRange(ctx, key)
	if err != nil {
		return err
	}
	targetNodeID, err := leaseholderNodeIDForRange(desc)
	if err != nil {
		return err
	}
	if targetNodeID == c.nodeID {
		_, err := c.host.PutIntentLocal(ctx, key, intent)
		return err
	}
	return c.postJSON(ctx, targetNodeID, "/control/kv/intent/put", kvIntentRequest{
		Key:    key,
		Intent: intent,
	}, nil)
}

func (c *kvClient) DeleteIntent(ctx context.Context, key []byte) error {
	desc, err := c.host.LookupRange(ctx, key)
	if err != nil {
		return err
	}
	targetNodeID, err := leaseholderNodeIDForRange(desc)
	if err != nil {
		return err
	}
	if targetNodeID == c.nodeID {
		_, err := c.host.DeleteIntentLocal(ctx, key)
		return err
	}
	return c.postJSON(ctx, targetNodeID, "/control/kv/intent/delete", kvGetRequest{Key: key}, nil)
}

func (c *kvClient) GetIntent(ctx context.Context, key []byte) (storage.Intent, bool, error) {
	desc, err := c.host.LookupRange(ctx, key)
	if err != nil {
		return storage.Intent{}, false, err
	}
	targetNodeID, err := leaseholderNodeIDForRange(desc)
	if err != nil {
		return storage.Intent{}, false, err
	}
	if targetNodeID == c.nodeID {
		intent, err := c.host.GetIntentLocal(ctx, key)
		switch {
		case err == nil:
			return intent, true, nil
		case err == storage.ErrIntentNotFound:
			return storage.Intent{}, false, nil
		default:
			return storage.Intent{}, false, err
		}
	}
	var resp kvIntentResponse
	if err := c.postJSON(ctx, targetNodeID, "/control/kv/intent/get", kvGetRequest{Key: key}, &resp); err != nil {
		return storage.Intent{}, false, err
	}
	return resp.Intent, resp.Found, nil
}

func (c *kvClient) RangeStatus(ctx context.Context, targetNodeID, rangeID uint64) (rangeStatusResponse, error) {
	if targetNodeID == c.nodeID {
		status, err := c.host.RangeStatus(rangeID)
		if err != nil {
			return rangeStatusResponse{}, err
		}
		return rangeStatusResponse{
			Hosted:           status.Hosted,
			LocalReplicaID:   status.LocalReplicaID,
			LeaderReplicaID:  status.LeaderReplicaID,
			AppliedIndex:     status.AppliedIndex,
			Descriptor:       status.Descriptor,
			DescriptorSource: status.DescriptorSource,
		}, nil
	}
	var resp rangeStatusResponse
	if err := c.postJSON(ctx, targetNodeID, "/control/range/status", rangeStatusRequest{RangeID: rangeID}, &resp); err != nil {
		return rangeStatusResponse{}, err
	}
	return resp, nil
}

func (c *kvClient) PrepareReplica(ctx context.Context, targetNodeID, rangeID, replicaID uint64, desc meta.RangeDescriptor, snapshot chronosruntime.ReplicaSnapshot) error {
	if targetNodeID == c.nodeID {
		return c.host.InstallReplicaSnapshot(ctx, rangeID, replicaID, snapshot)
	}
	return c.postJSON(ctx, targetNodeID, "/control/range/prepare", rangePrepareRequest{
		RangeID:    rangeID,
		ReplicaID:  replicaID,
		Descriptor: desc,
		Snapshot:   snapshot,
	}, nil)
}

func (c *kvClient) PutTxnRecord(ctx context.Context, record txn.Record) error {
	key := storage.GlobalTxnRecordKey(record.ID)
	desc, err := c.host.LookupRange(ctx, key)
	if err != nil {
		return err
	}
	targetNodeID, err := leaseholderNodeIDForRange(desc)
	if err != nil {
		return err
	}
	if targetNodeID == c.nodeID {
		_, err := c.host.PutTxnRecordLocal(ctx, record)
		return err
	}
	return c.postJSON(ctx, targetNodeID, "/control/kv/txn-record/put", kvTxnRecordRequest{Record: record}, nil)
}

func (c *kvClient) GetTxnRecord(ctx context.Context, txnID storage.TxnID) (txn.Record, bool, error) {
	key := storage.GlobalTxnRecordKey(txnID)
	desc, err := c.host.LookupRange(ctx, key)
	if err != nil {
		return txn.Record{}, false, err
	}
	targetNodeID, err := leaseholderNodeIDForRange(desc)
	if err != nil {
		return txn.Record{}, false, err
	}
	if targetNodeID == c.nodeID {
		record, err := c.host.GetTxnRecordLocal(ctx, txnID)
		if err != nil {
			return txn.Record{}, false, err
		}
		return record, true, nil
	}
	var resp kvTxnRecordResponse
	if err := c.postJSON(ctx, targetNodeID, "/control/kv/txn-record/get", kvGetRequest{Key: key}, &resp); err != nil {
		return txn.Record{}, false, err
	}
	return resp.Record, resp.Found, nil
}

func (c *kvClient) ScanTxnRecords(ctx context.Context) ([]txn.Record, error) {
	currentStart := storage.GlobalSystemTxnPrefix()
	endKey := storage.PrefixEnd(storage.GlobalSystemTxnPrefix())
	records := make([]txn.Record, 0)
	for {
		desc, err := c.host.LookupRange(ctx, currentStart)
		if err != nil {
			return nil, err
		}
		targetNodeID, err := leaseholderNodeIDForRange(desc)
		if err != nil {
			return nil, err
		}
		segmentEnd, _, done := clampScanEnd(desc, endKey, false)
		var resp kvTxnRecordScanResponse
		if targetNodeID == c.nodeID {
			localRecords, _, err := c.host.ScanTxnRecordsLocal(ctx, currentStart, segmentEnd)
			if err != nil {
				return nil, err
			}
			resp.Records = localRecords
		} else {
			if err := c.postJSON(ctx, targetNodeID, "/control/kv/txn-record/scan", kvTxnRecordScanRequest{
				StartKey: currentStart,
				EndKey:   segmentEnd,
			}, &resp); err != nil {
				return nil, err
			}
		}
		records = append(records, resp.Records...)
		if done || len(desc.EndKey) == 0 {
			return records, nil
		}
		currentStart = append([]byte(nil), desc.EndKey...)
	}
}

func (c *kvClient) ObserveIntents(ctx context.Context, refs []txn.IntentRef) ([]txn.ObservedIntent, error) {
	observed := make([]txn.ObservedIntent, 0, len(refs))
	for _, ref := range refs {
		intent, found, err := c.GetIntent(ctx, ref.Key)
		if err != nil {
			return nil, err
		}
		observed = append(observed, txn.ObservedIntent{
			Ref:       ref,
			Intent:    intent,
			Present:   found,
			Contested: false,
		})
	}
	return observed, nil
}

func (c *kvClient) ResolveTxnRecord(ctx context.Context, record txn.Record) error {
	required, err := intentSetFromRecord(record)
	if err != nil {
		return err
	}
	actions, err := txn.BuildAsyncResolution(record, required)
	if err != nil {
		return err
	}
	for _, action := range actions {
		intent, found, err := c.GetIntent(ctx, action.Ref.Key)
		if err != nil {
			return err
		}
		if found && (intent.TxnID != record.ID || intent.Epoch != record.Epoch) {
			found = false
		}
		if found && action.Kind == txn.ResolveActionCommit {
			if intent.Tombstone {
				if err := c.DeleteAt(ctx, action.Ref.Key, record.WriteTS); err != nil {
					return err
				}
			} else {
				if err := c.PutAt(ctx, action.Ref.Key, record.WriteTS, intent.Value); err != nil {
					return err
				}
			}
		}
		if found {
			if err := c.DeleteIntent(ctx, action.Ref.Key); err != nil {
				return err
			}
		}
		if err := c.ReleaseLock(ctx, action.Ref.Key, record.ID); err != nil {
			return err
		}
	}
	return nil
}

func (c *kvClient) AcquireLock(ctx context.Context, key []byte, record txn.Record, strength storage.IntentStrength) (txn.LockDecision, error) {
	desc, err := c.host.LookupRange(ctx, key)
	if err != nil {
		return txn.LockDecision{}, err
	}
	targetNodeID, err := leaseholderNodeIDForRange(desc)
	if err != nil {
		return txn.LockDecision{}, err
	}
	if targetNodeID == c.nodeID {
		var resp kvLockAcquireResponse
		if err := c.postJSON(ctx, targetNodeID, "/control/txn/lock/acquire", kvLockAcquireRequest{
			Key:      key,
			Txn:      record,
			Strength: strength,
		}, &resp); err != nil {
			return txn.LockDecision{}, err
		}
		return resp.Decision, nil
	}
	var resp kvLockAcquireResponse
	if err := c.postJSON(ctx, targetNodeID, "/control/txn/lock/acquire", kvLockAcquireRequest{
		Key:      key,
		Txn:      record,
		Strength: strength,
	}, &resp); err != nil {
		return txn.LockDecision{}, err
	}
	return resp.Decision, nil
}

func (c *kvClient) ReleaseLock(ctx context.Context, key []byte, txnID storage.TxnID) error {
	desc, err := c.host.LookupRange(ctx, key)
	if err != nil {
		return err
	}
	targetNodeID, err := leaseholderNodeIDForRange(desc)
	if err != nil {
		return err
	}
	if targetNodeID == c.nodeID {
		return c.postJSON(ctx, targetNodeID, "/control/txn/lock/release", kvLockReleaseRequest{
			Key:   key,
			TxnID: txnID,
		}, nil)
	}
	return c.postJSON(ctx, targetNodeID, "/control/txn/lock/release", kvLockReleaseRequest{
		Key:   key,
		TxnID: txnID,
	}, nil)
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
