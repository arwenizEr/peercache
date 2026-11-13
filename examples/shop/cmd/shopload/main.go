// Command shopload drives the shop demo and checks what it promises.
//
//	shopload run   -target URL -pods URL,URL,... -duration 10m -ledger dir
//	shopload check -target URL -ledger dir
//	shopload stale -pods URL,URL,... -rounds 50
//	shopload limit -target URL
//
// run writes every checkout's outcome to the ledger: acknowledged (201 or a
// retry's 200), refused (409, out of stock), or uncertain (never answered
// definitively — it may or may not have happened). check then asserts that no
// acknowledged order is lost and that stock agrees with the ledger, counting
// the uncertain ones as the only slack. stale measures how long a catalog
// update takes to reach every pod. limit checks that a rate limit admits
// exactly its quota across all pods.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Must match examples/shop.
const (
	initialStock = 20_000
	rateLimit    = 6000
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: shopload run|check|stale|limit [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = run(os.Args[2:])
	case "check":
		err = check(os.Args[2:])
	case "stale":
		err = stale(os.Args[2:])
	case "limit":
		err = limit(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "shopload:", err)
		os.Exit(1)
	}
}

// client keeps a connection per worker alive. The default transport keeps two
// idle connections per host, so a few dozen workers dialled afresh for nearly
// every request; on Windows that churn exhausted the ephemeral ports (TIME_WAIT
// holds each for two minutes), every dial then failed in a blocking
// closesocket, and the runtime's 10,000-thread limit ended the run.
var client = &http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 256,
		// Bounded, too. Unbounded, every request that timed out while ports
		// were exhausted started another dial, and ten thousand of them sat in
		// closesocket at once.
		MaxConnsPerHost: 256,
		IdleConnTimeout: 90 * time.Second,
	},
}

func do(method, url, user string) (int, string, time.Duration, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return 0, "", 0, err
	}
	if user != "" {
		req.Header.Set("X-User", user)
	}
	start := time.Now()
	resp, err := client.Do(req)
	took := time.Since(start)
	if err != nil {
		return 0, "", took, err
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, string(body), took, nil
}

// stats collects per-operation outcomes and latencies.
type stats struct {
	mu   sync.Mutex
	ops  map[string]map[int]int
	lat  map[string][]time.Duration
	errs map[string]int
}

func newStats() *stats {
	return &stats{ops: map[string]map[int]int{}, lat: map[string][]time.Duration{}, errs: map[string]int{}}
}

func (s *stats) add(op string, status int, took time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.errs[op]++
		return
	}
	if s.ops[op] == nil {
		s.ops[op] = map[int]int{}
	}
	s.ops[op][status]++
	s.lat[op] = append(s.lat[op], took)
}

func (s *stats) report(w io.Writer, elapsed time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.ops))
	for op := range s.ops {
		names = append(names, op)
	}
	sort.Strings(names)
	fmt.Fprintf(w, "%-10s %8s %9s %9s %9s  statuses\n", "op", "count", "rps", "p50", "p99")
	for _, op := range names {
		lat := s.lat[op]
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		n := len(lat)
		var codes []string
		for code, c := range s.ops[op] {
			codes = append(codes, fmt.Sprintf("%d:%d", code, c))
		}
		sort.Strings(codes)
		fmt.Fprintf(w, "%-10s %8d %9.1f %9s %9s  %s errors:%d\n", op, n, float64(n)/elapsed.Seconds(),
			lat[n/2].Round(10*time.Microsecond), lat[n*99/100].Round(10*time.Microsecond),
			strings.Join(codes, " "), s.errs[op])
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	target := fs.String("target", "http://localhost:8080", "shop service URL")
	duration := fs.Duration("duration", time.Minute, "how long to run")
	workers := fs.Int("workers", 32, "concurrent clients")
	users := fs.Int("users", 2000, "distinct users")
	products := fs.Int("products", 500, "distinct products")
	items := fs.Int("items", 50, "distinct stock items")
	ledgerDir := fs.String("ledger", "ledger", "directory for the checkout ledger")
	_ = fs.Parse(args)

	if err := os.MkdirAll(*ledgerDir, 0o755); err != nil {
		return err
	}
	open := func(name string) (*os.File, error) {
		return os.OpenFile(filepath.Join(*ledgerDir, name), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	}
	acked, err := open("acknowledged")
	if err != nil {
		return err
	}
	defer acked.Close()
	refused, err := open("refused")
	if err != nil {
		return err
	}
	defer refused.Close()
	uncertain, err := open("uncertain")
	if err != nil {
		return err
	}
	defer uncertain.Close()
	// attempted is written before each checkout is sent, so a checkout in
	// flight when this process dies is still accounted for: the checker
	// treats an attempt without an outcome as uncertain.
	attempted, err := open("attempted")
	if err != nil {
		return err
	}
	defer attempted.Close()
	var ledgerMu sync.Mutex
	write := func(f *os.File, line string) {
		ledgerMu.Lock()
		fmt.Fprintln(f, line)
		ledgerMu.Unlock()
	}

	st := newStats()
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	var seq atomic.Int64
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), uint64(start.UnixNano())))
			for ctx.Err() == nil {
				user := fmt.Sprintf("u%d", r.IntN(*users))
				switch p := r.IntN(100); {
				case p < 60: // catalog, a fifth of it one hot product
					id := fmt.Sprintf("p%d", r.IntN(*products))
					if r.IntN(5) == 0 {
						id = "p0"
					}
					code, _, took, err := do("GET", *target+"/product/"+id, user)
					st.add("product", code, took, err)
				case p < 75:
					code, _, took, err := do("POST", fmt.Sprintf("%s/cart/%s/add?item=i%d", *target, user, r.IntN(*items)), user)
					st.add("cart-add", code, took, err)
				case p < 80:
					code, _, took, err := do("GET", *target+"/cart/"+user, user)
					st.add("cart-get", code, took, err)
				default:
					item := fmt.Sprintf("i%d", r.IntN(*items))
					order := fmt.Sprintf("%s-%d", runID, seq.Add(1))
					write(attempted, order+" "+item)
					outcome := checkout(st, *target, order, item, user)
					switch outcome {
					case "acknowledged":
						write(acked, order+" "+item)
					case "refused":
						write(refused, order+" "+item)
					case "refused-rate":
						// Rate limited before anything was attempted.
					default:
						write(uncertain, order+" "+item)
					}
				}
			}
		}(w)
	}
	wg.Wait()
	st.report(os.Stdout, time.Since(start))
	return nil
}

// checkout retries an unanswered checkout under the same order ID, which the
// shop treats as an idempotency key, and reports how it ended.
func checkout(st *stats, target, order, item, user string) string {
	// Retries span about six seconds, like a real client's: long enough to
	// ride out a failover's "ask again", which is what a 503 means here.
	for attempt := 0; attempt < 7; attempt++ {
		code, _, took, err := do("POST", fmt.Sprintf("%s/checkout/%s?item=%s", target, order, item), user)
		st.add("checkout", code, took, err)
		switch {
		case err == nil && (code == http.StatusCreated || code == http.StatusOK):
			return "acknowledged"
		case err == nil && code == http.StatusConflict:
			return "refused"
		case err == nil && code == http.StatusTooManyRequests:
			return "refused-rate" // nothing was attempted
		}
		time.Sleep(time.Duration(200*(attempt+1)) * time.Millisecond)
	}
	return "uncertain"
}

func readLedger(dir, name string) ([][2]string, error) {
	f, err := os.Open(filepath.Join(dir, name))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out [][2]string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if parts := strings.Fields(sc.Text()); len(parts) == 2 {
			out = append(out, [2]string{parts[0], parts[1]})
		}
	}
	return out, sc.Err()
}

func check(args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	target := fs.String("target", "http://localhost:8080", "shop service URL")
	ledgerDir := fs.String("ledger", "ledger", "the run's ledger")
	_ = fs.Parse(args)

	acked, err := readLedger(*ledgerDir, "acknowledged")
	if err != nil {
		return err
	}
	uncertain, err := readLedger(*ledgerDir, "uncertain")
	if err != nil {
		return err
	}
	refused, err := readLedger(*ledgerDir, "refused")
	if err != nil {
		return err
	}
	attempted, err := readLedger(*ledgerDir, "attempted")
	if err != nil {
		return err
	}
	// An attempt with no outcome died with the load generator: uncertain.
	settled := map[string]bool{}
	for _, list := range [][][2]string{acked, refused, uncertain} {
		for _, o := range list {
			settled[o[0]] = true
		}
	}
	for _, o := range attempted {
		if !settled[o[0]] {
			uncertain = append(uncertain, o)
		}
	}

	// 1. No acknowledged order is lost.
	var lost []string
	perItem := map[string]int{}
	for _, o := range acked {
		perItem[o[1]]++
		code, _, _, err := retryGet(*target + "/order/" + o[0])
		if err != nil || code != http.StatusOK {
			lost = append(lost, fmt.Sprintf("%s(%d,%v)", o[0], code, err))
		}
	}
	slack := map[string]int{}
	for _, o := range uncertain {
		slack[o[1]]++
	}

	// 2. Stock agrees with the ledger: initial minus acknowledged orders,
	// give or take the uncertain ones for that item.
	items := map[string]bool{}
	for it := range perItem {
		items[it] = true
	}
	for it := range slack {
		items[it] = true
	}
	var wrong []string
	exact := 0
	for it := range items {
		code, body, _, err := retryGet(*target + "/stock/" + it)
		if err != nil || code != http.StatusOK {
			wrong = append(wrong, fmt.Sprintf("%s: unreadable (%d %v)", it, code, err))
			continue
		}
		got, _ := strconv.Atoi(strings.TrimSpace(body))
		want := initialStock - perItem[it]
		if got < 0 || got > want || got < want-slack[it] {
			wrong = append(wrong, fmt.Sprintf("%s: stock %d, ledger says %d (uncertain %d)", it, got, want, slack[it]))
		} else if got == want {
			exact++
		}
	}

	fmt.Printf("acknowledged orders: %d, lost: %d\n", len(acked), len(lost))
	fmt.Printf("uncertain checkouts: %d\n", len(uncertain))
	fmt.Printf("stock items: %d, exact: %d, within uncertainty: %d, wrong: %d\n",
		len(items), exact, len(items)-exact-len(wrong), len(wrong))
	for i, l := range lost {
		if i == 20 {
			fmt.Printf("  ... and %d more lost\n", len(lost)-20)
			break
		}
		fmt.Println("  LOST", l)
	}
	for _, w := range wrong {
		fmt.Println("  WRONG", w)
	}
	if len(lost) > 0 || len(wrong) > 0 {
		return fmt.Errorf("%d lost orders, %d wrong stock counts", len(lost), len(wrong))
	}
	return nil
}

// retryGet reads through transient unavailability: a 503 means "ask again"
// during a failover, not "gone".
func retryGet(url string) (int, string, time.Duration, error) {
	var code int
	var body string
	var took time.Duration
	var err error
	for attempt := 0; attempt < 10; attempt++ {
		code, body, took, err = do("GET", url, "checker")
		if err == nil && code != http.StatusServiceUnavailable && code != http.StatusGatewayTimeout {
			return code, body, took, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return code, body, took, err
}

// stale updates a product and measures how long until every pod serves the
// new price.
func stale(args []string) error {
	fs := flag.NewFlagSet("stale", flag.ExitOnError)
	pods := fs.String("pods", "", "comma-separated pod URLs")
	rounds := fs.Int("rounds", 30, "updates to measure")
	_ = fs.Parse(args)
	urls := strings.Split(*pods, ",")
	if *pods == "" || len(urls) < 2 {
		return errors.New("-pods needs at least two pod URLs")
	}
	var waits []time.Duration
	for i := 0; i < *rounds; i++ {
		price := strconv.FormatInt(time.Now().UnixNano()%1_000_000_000, 10)
		writer := urls[i%len(urls)]
		code, _, _, err := do("PUT", writer+"/product/probe?price="+price, "prober")
		if err != nil || code != http.StatusNoContent {
			return fmt.Errorf("update on %s: %d %v", writer, code, err)
		}
		written := time.Now()
		for _, u := range urls {
			for {
				_, body, _, err := do("GET", u+"/product/probe", "prober")
				if err == nil && strings.Contains(body, `"price":`+price+`}`) {
					break
				}
				if time.Since(written) > 30*time.Second {
					return fmt.Errorf("%s never served price %s", u, price)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		waits = append(waits, time.Since(written))
	}
	sort.Slice(waits, func(i, j int) bool { return waits[i] < waits[j] })
	fmt.Printf("catalog update visible on all %d pods: p50 %v, max %v over %d updates\n",
		len(urls), waits[len(waits)/2].Round(time.Millisecond), waits[len(waits)-1].Round(time.Millisecond), len(waits))
	return nil
}

// limit sends more than the quota for one user, concurrently across the
// service, and checks exactly the quota is admitted.
func limit(args []string) error {
	fs := flag.NewFlagSet("limit", flag.ExitOnError)
	target := fs.String("target", "http://localhost:8080", "shop service URL")
	_ = fs.Parse(args)
	user := "limit-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	var ok, limited, other atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	total := rateLimit + 500
	// Start just after a window boundary: a burst that straddles two
	// windows is rightly admitted twice, and would read as a failure.
	now := time.Now()
	time.Sleep(time.Until(now.Truncate(time.Minute).Add(time.Minute + time.Second)))
	start := time.Now()
	for i := 0; i < total; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			code, _, _, err := do("GET", *target+"/product/p1", user)
			switch {
			case err == nil && code == http.StatusOK:
				ok.Add(1)
			case err == nil && code == http.StatusTooManyRequests:
				limited.Add(1)
			default:
				other.Add(1)
			}
		}()
	}
	wg.Wait()
	if time.Since(start) > 50*time.Second {
		fmt.Println("warning: the burst ran past its one-minute window; the result is not meaningful")
	}
	fmt.Printf("rate limit %d: admitted %d, limited %d, other %d\n", rateLimit, ok.Load(), limited.Load(), other.Load())
	if ok.Load()+other.Load() < rateLimit || ok.Load() > rateLimit {
		return fmt.Errorf("admitted %d of a %d quota", ok.Load(), rateLimit)
	}
	return nil
}
