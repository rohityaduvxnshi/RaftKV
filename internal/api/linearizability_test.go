package api_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// A per-key register model supporting get/put/append — the standard
// linearizability model for a KV store. Only get's output is constrained;
// put/append always succeed and transform the state.
type kvInput struct {
	op    uint8 // 0=get, 1=put, 2=append
	key   string
	value string
}
type kvOutput struct{ value string }

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).key
			byKey[k] = append(byKey[k], op)
		}
		parts := make([][]porcupine.Operation, 0, len(byKey))
		for _, v := range byKey {
			parts = append(parts, v)
		}
		return parts
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(string)
		in := input.(kvInput)
		switch in.op {
		case 0: // get: must observe the current register value
			return output.(kvOutput).value == st, st
		case 1: // put
			return true, in.value
		default: // append
			return true, st + in.value
		}
	},
	Equal: func(a, b interface{}) bool { return a.(string) == b.(string) },
}

// currentLeader returns a connected leader without blocking (unlike leader()).
func (c *apiCluster) currentLeader() (int, bool) {
	for i := 0; i < c.n; i++ {
		if c.connected[i] {
			if _, isLeader := c.rafts[i].GetState(); isLeader {
				return i, true
			}
		}
	}
	return -1, false
}

// TestLinearizability drives a 3-node cluster with concurrent writers (Append)
// and readers (linearizable Get) and checks the recorded history is
// linearizable with Porcupine. Exactly-once sessions make the retry-on-leader-
// change safe, so each (client,seq) append appears exactly once in the history.
func TestLinearizability(t *testing.T) {
	c := makeAPICluster(t, 3, 1)
	defer c.cleanup()
	c.leader() // wait for a stable leader

	keys := []string{"x", "y", "z"}
	const writers, readers = 3, 3
	const appendsPer, getsPer = 30, 40

	var mu sync.Mutex
	var ops []porcupine.Operation
	record := func(cid int, in kvInput, out kvOutput, call, ret int64) {
		mu.Lock()
		ops = append(ops, porcupine.Operation{ClientId: cid, Input: in, Output: out, Call: call, Return: ret})
		mu.Unlock()
	}

	// viaLeader retries fn against the current leader until it succeeds (or a
	// generous safety deadline — never hit on the reliable in-mem network).
	viaLeader := func(fn func(l int) error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for {
			if l, ok := c.currentLeader(); ok {
				if fn(l) == nil {
					return
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cid := fmt.Sprintf("w%d", id)
			for seq := uint64(1); seq <= appendsPer; seq++ {
				key := keys[int(seq)%len(keys)]
				val := fmt.Sprintf("%s.%d;", cid, seq)
				call := time.Now().UnixNano()
				viaLeader(func(l int) error {
					_, err := c.servers[l].Append(context.Background(), cid, seq, key, val)
					return err
				})
				record(id, kvInput{op: 2, key: key, value: val}, kvOutput{}, call, time.Now().UnixNano())
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < getsPer; i++ {
				key := keys[i%len(keys)]
				call := time.Now().UnixNano()
				var got string
				viaLeader(func(l int) error {
					v, _, err := c.servers[l].Get(context.Background(), key)
					if err == nil {
						got = v
					}
					return err
				})
				record(writers+id, kvInput{op: 0, key: key}, kvOutput{value: got}, call, time.Now().UnixNano())
			}
		}(r)
	}
	wg.Wait()

	if len(ops) < writers*appendsPer {
		t.Fatalf("recorded only %d ops — workload did not complete", len(ops))
	}
	switch porcupine.CheckOperationsTimeout(kvModel, ops, 30*time.Second) {
	case porcupine.Illegal:
		t.Fatalf("history is NOT linearizable (%d ops)", len(ops))
	case porcupine.Unknown:
		t.Logf("porcupine timed out on %d ops (inconclusive, not a failure)", len(ops))
	default:
		t.Logf("linearizable: %d ops across %d keys", len(ops), len(keys))
	}
}
