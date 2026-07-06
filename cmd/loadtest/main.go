// Command loadtest fires concurrent PUTs at a RaftKV node's HTTP API and reports
// write throughput and client-side latency percentiles. Point -addr at the
// current leader (a follower 307-redirects to container hostnames the host can't
// resolve). Find it with: chaos/lib.sh find_leader, or probe /kv/_probe for 200.
package main

import (
	"flag"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("addr", "http://127.0.0.1:8080", "leader HTTP base URL")
	conc := flag.Int("c", 16, "concurrent clients")
	dur := flag.Duration("d", 5*time.Second, "test duration")
	flag.Parse()

	client := &http.Client{Timeout: 5 * time.Second}
	var ok, fail int64
	var mu sync.Mutex
	var lat []time.Duration
	deadline := time.Now().Add(*dur)

	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cid := fmt.Sprintf("load-%d", id)
			local := make([]time.Duration, 0, 4096)
			for seq := 1; time.Now().Before(deadline); seq++ {
				req, _ := http.NewRequest("PUT", *addr+"/kv/loadk", strings.NewReader(fmt.Sprintf("v%d", seq)))
				req.Header.Set("X-Client-Id", cid)
				req.Header.Set("X-Seq-No", fmt.Sprint(seq))
				t0 := time.Now()
				resp, err := client.Do(req)
				d := time.Since(t0)
				if err == nil && resp.StatusCode == http.StatusNoContent {
					atomic.AddInt64(&ok, 1)
					local = append(local, d)
				} else {
					atomic.AddInt64(&fail, 1)
				}
				if resp != nil {
					resp.Body.Close()
				}
			}
			mu.Lock()
			lat = append(lat, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[int(float64(len(lat)-1)*p)].Round(time.Microsecond)
	}
	fmt.Printf("duration=%v concurrency=%d\n", elapsed.Round(time.Millisecond), *conc)
	fmt.Printf("ok=%d fail=%d  throughput=%.0f writes/sec\n", ok, fail, float64(ok)/elapsed.Seconds())
	fmt.Printf("latency p50=%v p99=%v p99.9=%v max=%v\n", pct(0.50), pct(0.99), pct(0.999), pct(1.0))
}
