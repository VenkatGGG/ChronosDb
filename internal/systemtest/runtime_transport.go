package systemtest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	chronosruntime "github.com/VenkatGGG/ChronosDb/internal/runtime"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type processNodeTransport struct {
	selfNodeID uint64
	rootDir    string
	client     *http.Client
}

type raftMessageBatchRequest struct {
	Messages []raftMessageRequest `json:"messages"`
}

type raftMessageRequest struct {
	RangeID uint64 `json:"range_id"`
	Message []byte `json:"message"`
}

func newProcessNodeTransport(selfNodeID uint64, rootDir string) chronosruntime.Transport {
	return &processNodeTransport{
		selfNodeID: selfNodeID,
		rootDir:    rootDir,
		client:     &http.Client{Timeout: 2 * time.Second},
	}
}

func (t *processNodeTransport) Send(ctx context.Context, messages []chronosruntime.OutboundMessage) error {
	grouped := make(map[uint64][]raftMessageRequest)
	for _, outbound := range messages {
		if outbound.TargetNodeID == 0 || outbound.TargetNodeID == t.selfNodeID {
			continue
		}
		payload, err := outbound.Message.Marshal()
		if err != nil {
			return err
		}
		grouped[outbound.TargetNodeID] = append(grouped[outbound.TargetNodeID], raftMessageRequest{
			RangeID: outbound.RangeID,
			Message: payload,
		})
	}

	for nodeID, batch := range grouped {
		controlURL, err := t.resolveControlURL(nodeID)
		if err != nil {
			return err
		}
		body, err := json.Marshal(raftMessageBatchRequest{Messages: batch})
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, controlURL+"/control/raft/message", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := t.client.Do(req)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		if resp.StatusCode >= 300 {
			return fmt.Errorf("runtime transport: raft message post to node %d returned status %d", nodeID, resp.StatusCode)
		}
	}
	return nil
}

func (t *processNodeTransport) resolveControlURL(targetNodeID uint64) (string, error) {
	entries, err := os.ReadDir(t.rootDir)
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		statePath := filepath.Join(t.rootDir, entry.Name(), "state.json")
		state, err := ReadProcessNodeState(statePath)
		if err != nil {
			continue
		}
		if state.NodeID == targetNodeID && state.ControlURL != "" {
			return state.ControlURL, nil
		}
	}
	return "", fmt.Errorf("runtime transport: node %d control url not found under %s", targetNodeID, t.rootDir)
}

func unmarshalRaftMessage(data []byte) (raftpb.Message, error) {
	var message raftpb.Message
	if err := message.Unmarshal(data); err != nil {
		return raftpb.Message{}, err
	}
	return message, nil
}
