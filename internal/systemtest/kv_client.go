package systemtest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/meta"
	chronosruntime "github.com/VenkatGGG/ChronosDb/internal/runtime"
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
	desc, err := c.host.LookupRange(ctx, key)
	if err != nil {
		return err
	}
	targetNodeID, err := leaseholderNodeIDForRange(desc)
	if err != nil {
		return err
	}
	ts := hlc.Timestamp{WallTime: uint64(time.Now().UTC().UnixNano())}
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

func leaseholderNodeIDForRange(desc meta.RangeDescriptor) (uint64, error) {
	for _, replica := range desc.Replicas {
		if replica.ReplicaID == desc.LeaseholderReplicaID {
			return replica.NodeID, nil
		}
	}
	return 0, fmt.Errorf("systemtest kv client: leaseholder replica %d not found for range %d", desc.LeaseholderReplicaID, desc.RangeID)
}
