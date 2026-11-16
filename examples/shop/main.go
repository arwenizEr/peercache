// Command shop is a demo service that uses every PeerCache feature at once, the
// way an application would. It exists for M16's real-world validation
// (docs/decisions/0033-real-world-validation.md): run as a StatefulSet on
// Kubernetes, under load, through pod kills, restarts and partitions.
//
// Endpoints:
//
//	GET  /product/{id}           catalog: replica reads, a loader behind them
//	PUT  /product/{id}?price=N   update a product (staleness is measured on this)
//	POST /cart/{user}/add?item=I a group per user, Call
//	GET  /cart/{user}
//	POST /checkout/{order}?item=I  idempotent order, stock by Increment; see checkout
//	GET  /order/{order}
//	GET  /stock/{item}
//	GET  /metrics, /debug/peercache/, /healthz
//
// Every request also passes a per-user rate limit, an Increment with a TTL.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hashicorp/memberlist"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/membership"
	"github.com/arwenizEr/peercache/persist"
	pcs3 "github.com/arwenizEr/peercache/persist/s3"
	"github.com/arwenizEr/peercache/transport"
)

// The shop's fixed shape. The checker reads the same constants.
const (
	InitialStock = 20_000
	RateLimit    = 6000 // requests per user per minute
)

type config struct {
	nodeID, podIP, seedDNS                string
	s3Endpoint, s3Bucket, s3Key, s3Secret string
	httpAddr                              string
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func main() {
	cfg := config{
		nodeID:     env("NODE_ID", "shop-0"),
		podIP:      env("POD_IP", "127.0.0.1"),
		seedDNS:    env("SEED_DNS", ""),
		s3Endpoint: env("S3_ENDPOINT", ""),
		s3Bucket:   env("S3_BUCKET", "peercache"),
		s3Key:      env("S3_ACCESS_KEY", ""),
		s3Secret:   env("S3_SECRET_KEY", ""),
		httpAddr:   env("HTTP_ADDR", ":8080"),
	}
	if err := run(cfg); err != nil {
		fatal(err)
	}
}

func run(cfg config) error {
	// The data port, bound here so a failure is reported where it can be
	// acted on. Peers learn the address through gossip metadata.
	gossipPort, _ := strconv.Atoi(env("GOSSIP_PORT", "7946"))
	dataPort := env("DATA_PORT", "7947")
	l, err := net.Listen("tcp", ":"+dataPort)
	if err != nil {
		return fmt.Errorf("listening for peers: %w", err)
	}
	self := peercache.Node{ID: cfg.nodeID, Addr: net.JoinHostPort(cfg.podIP, dataPort)}
	tr, err := transport.New(transport.Options{Self: self})
	if err != nil {
		return err
	}
	mcfg := memberlist.DefaultLANConfig()
	mcfg.Name = cfg.nodeID
	mcfg.BindPort = gossipPort
	mcfg.AdvertiseAddr = cfg.podIP
	mcfg.AdvertisePort = gossipPort
	gossip, err := membership.New(membership.Options{Self: self, Config: mcfg})
	if err != nil {
		return err
	}

	cache, err := peercache.New(
		peercache.WithNodeID(cfg.nodeID),
		peercache.WithMembership(gossip),
		peercache.WithTransport(tr),
		peercache.WithListener(l),
		peercache.WithMemoryBudget(512<<20),
		peercache.WithLogger(stderrLogger{}),
		// The shop calls CatchUp before it is ready (catchUpLoop), so peers'
		// writes to keys it owns can wait for it: without this, kill -9 lost
		// 1 to 3 stock decrements on 5 of 50 counters (docs/api.md,
		// "WithCatchUpGate").
		peercache.WithCatchUpGate(),
	)
	if err != nil {
		return err
	}
	// Namespaces first, then the cluster. A node that joins before opening
	// them is written to by its peers for namespaces it has no options for;
	// the library refuses those requests until it opens them, and before
	// that it created them with defaults: no replication, no persistence. On
	// kind a pod restarted while MinIO was unreachable, waited for the bucket
	// here after joining, and lost its share of every acknowledged order
	// (docs/decisions/open.md question 32). Catch-up also waits: it pulls
	// only for namespaces that are open.
	store, err := blobStore(cfg)
	if err != nil {
		return err
	}
	s, err := newShop(cache, store)
	if err != nil {
		return err
	}
	go joinLoop(gossip, cfg)
	go catchUpLoop(cache)

	mux := http.NewServeMux()
	s.routes(mux)
	mux.Handle("GET /metrics", cache.MetricsHandler())
	mux.Handle("/debug/peercache/", http.StripPrefix("/debug/peercache", cache.DebugHandler()))
	// Profiles, because M16 is where the library first runs at a scale whose
	// costs are worth looking at.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		r := cache.Ready()
		if !r.Ready {
			http.Error(w, r.Reason, http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: cfg.httpAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	errs := make(chan error, 1)
	go func() { errs <- srv.ListenAndServe() }()
	logf("shop %s serving on %s, peers on %s", cfg.nodeID, cfg.httpAddr, self.Addr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	select {
	case err := <-errs:
		return err
	case <-sig:
	}
	// The graceful path: drain hands owned keys and flushes persistence,
	// then the HTTP server and the cache close.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	if err := cache.Drain(ctx); err != nil {
		logf("drain: %v", err)
	}
	_ = srv.Shutdown(ctx)
	return cache.Close(ctx)
}

// catchUpLoop pulls the replicas this node should hold: once the membership
// has settled after start — readiness reports false until it has — and again
// after every membership change, which is what restores the replication factor
// (docs/api.md, "CatchUp").
func catchUpLoop(cache peercache.Cache) {
	var done uint64 = ^uint64(0)
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for range tick.C {
		st := cache.Stats()
		r := cache.Ready()
		if !r.Joined || !r.Settled || uint64(st.Epoch) == done {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		err := cache.CatchUp(ctx)
		cancel()
		if err != nil {
			logf("catch-up incomplete, will retry: %v", err)
			continue
		}
		done = uint64(st.Epoch)
	}
}

// joinLoop joins the cluster and then keeps joining it. memberlist does not
// reconnect to a peer it has declared dead, so after a partition heals the two
// sides would stay two clusters — each owning every key — until something
// introduces them again. Re-joining the seeds every few seconds is that
// something; joining a peer already in the view is a cheap state exchange.
func joinLoop(g *membership.Gossip, cfg config) {
	if explicit := env("SEEDS", ""); explicit != "" {
		for g.Join(strings.Split(explicit, ",")) != nil {
			<-time.After(time.Second)
		}
		return
	}
	if cfg.seedDNS == "" {
		return
	}
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for ; ; <-tick.C {
		ips, err := net.LookupHost(cfg.seedDNS)
		var seeds []string
		for _, ip := range ips {
			if ip != cfg.podIP {
				seeds = append(seeds, net.JoinHostPort(ip, "7946"))
			}
		}
		if err == nil && len(seeds) > 0 {
			_ = g.Join(seeds)
		}
	}
}

func blobStore(cfg config) (persist.BlobStore, error) {
	if cfg.s3Endpoint == "" {
		dir := env("DATA_DIR", "/tmp/shop-data")
		return persist.Dir(dir)
	}
	client := awss3.New(awss3.Options{
		BaseEndpoint: aws.String(cfg.s3Endpoint),
		Region:       "us-east-1",
		UsePathStyle: true,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: cfg.s3Key, SecretAccessKey: cfg.s3Secret}, nil
		}),
	})
	ensureBucket(client, cfg.s3Bucket)
	return pcs3.New(client, cfg.s3Bucket, "shop"), nil
}

// ensureBucket creates the bucket if it is missing, so the demo needs nothing
// but an S3-compatible endpoint. Every pod tries; all but the first are told
// it exists. It gives up quietly after a minute: a store that is down at start
// is a scenario, not a reason to refuse to serve.
func ensureBucket(client *awss3.Client, bucket string) {
	for attempt := 0; attempt < 30; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(bucket)})
		if err == nil {
			cancel()
			return
		}
		_, err = client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
		cancel()
		if err == nil {
			logf("created bucket %q", bucket)
			return
		}
		logf("bucket %q not ready: %v", bucket, err)
		<-time.After(2 * time.Second)
	}
}

type shop struct {
	cache                         peercache.Cache
	catalog, stock, orders, limit peercache.Namespace
}

func newShop(cache peercache.Cache, store persist.BlobStore) (*shop, error) {
	durable := persist.WithFlushInterval(time.Second)
	s := &shop{
		cache: cache,
		// A hot, read-mostly key space: replicas answer reads, a loader
		// stands in for the database.
		catalog: cache.Namespace("catalog",
			peercache.WithReplicationFactor(3),
			peercache.WithReplicaReads(),
			peercache.WithL1TTL(2*time.Second),
			// Product IDs are not personal data, so the dashboard may name
			// the hot ones (docs/decisions/0040-hot-keys.md).
			peercache.WithHotKeyTracking(10),
			peercache.WithLoader(func(_ context.Context, key string) ([]byte, time.Duration, error) {
				return []byte(`{"id":"` + key + `","price":100}`), 0, nil
			})),
		stock: cache.Namespace("stock",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication(),
			peercache.WithPersistence(store, durable)),
		orders: cache.Namespace("orders",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication(),
			peercache.WithPersistence(store, durable)),
		limit: cache.Namespace("ratelimit", peercache.WithReplicationFactor(2)),
	}
	for name, fn := range map[string]peercache.GroupFunc{"add": cartAdd, "get": cartGet} {
		if err := cache.RegisterGroupFunc(name, fn); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func cartAdd(_ context.Context, st peercache.GroupState, arg []byte) ([]byte, error) {
	n := 0
	if v, ok := st.Get(string(arg)); ok {
		n, _ = strconv.Atoi(string(v))
	}
	st.Set(string(arg), []byte(strconv.Itoa(n+1)))
	return []byte(strconv.Itoa(n + 1)), nil
}

func cartGet(_ context.Context, st peercache.GroupState, _ []byte) ([]byte, error) {
	items := map[string]string{}
	for _, k := range st.Keys() {
		v, _ := st.Get(k)
		items[k] = string(v)
	}
	return json.Marshal(items)
}

func (s *shop) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /product/{id}", s.limited(s.getProduct))
	mux.HandleFunc("PUT /product/{id}", s.limited(s.putProduct))
	mux.HandleFunc("POST /cart/{user}/add", s.limited(s.cartAdd))
	mux.HandleFunc("GET /cart/{user}", s.limited(s.cartGet))
	mux.HandleFunc("POST /checkout/{order}", s.limited(s.checkout))
	mux.HandleFunc("GET /order/{order}", s.getOrder)
	mux.HandleFunc("GET /stock/{item}", s.getStock)
}

// limited applies the per-user rate limit: one Increment per request on a key
// that expires with its minute.
func (s *shop) limited(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := r.Header.Get("X-User")
		if user == "" {
			user = "anonymous"
		}
		window := time.Now().Unix() / 60
		n, err := s.limit.Increment(r.Context(), fmt.Sprintf("%s:%d", user, window), 1, 2*time.Minute)
		if classify(err) != applied {
			fail(w, err)
			return
		}
		if n > RateLimit {
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		h(w, r)
	}
}

func (s *shop) getProduct(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	v, err := s.catalog.Get(r.Context(), id)
	if errors.Is(err, peercache.ErrNotFound) {
		v, err = s.catalog.GetOrLoad(r.Context(), id)
	}
	if err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(v)
}

func (s *shop) putProduct(w http.ResponseWriter, r *http.Request) {
	id, price := r.PathValue("id"), r.URL.Query().Get("price")
	if _, err := strconv.Atoi(price); err != nil {
		http.Error(w, "price must be an integer", http.StatusBadRequest)
		return
	}
	if err := s.catalog.Set(r.Context(), id, []byte(`{"id":"`+id+`","price":`+price+`}`), 0); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *shop) cartAdd(w http.ResponseWriter, r *http.Request) {
	item := r.URL.Query().Get("item")
	if item == "" {
		http.Error(w, "item is required", http.StatusBadRequest)
		return
	}
	n, err := s.cache.Group("cart:"+r.PathValue("user")).Call(r.Context(), "add", []byte(item))
	if err != nil {
		fail(w, err)
		return
	}
	_, _ = w.Write(n)
}

func (s *shop) cartGet(w http.ResponseWriter, r *http.Request) {
	v, err := s.cache.Group("cart:"+r.PathValue("user")).Call(r.Context(), "get", nil)
	if err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(v)
}

// Order records carry a state, because the order key and the stock key are two
// keys and there is no transaction across them — by design there are none. So
// an order cannot simply exist or not: it is pending from the moment it is
// claimed until the stock decrement is known to have happened, and only then
// placed. A pending order whose decrement is unknown stays pending: deleting it
// would let a retry decrement again, and placing it would claim a decrement
// that may not have happened.
const (
	statePending = "pending"
	statePlaced  = "placed"
	stateRefused = "refused"
)

// outcome sorts a write's error the way an application must before acting on
// a non-idempotent operation.
type outcome int

const (
	applied outcome = iota // nil, or ErrUnderReplicated: the write happened
	refused                // answered before anything was applied
	unknown                // anything else: it may have happened
)

func classify(err error) outcome {
	switch {
	case err == nil, errors.Is(err, peercache.ErrUnderReplicated):
		return applied
	case errors.Is(err, peercache.ErrNotOwner), errors.Is(err, peercache.ErrStaleEpoch),
		errors.Is(err, peercache.ErrActivationPending), errors.Is(err, peercache.ErrGroupOverloaded),
		errors.Is(err, peercache.ErrClosed):
		return refused
	default:
		return unknown
	}
}

// checkout places one order for one unit of item. The order ID is the client's
// idempotency key.
//
//	201  placed by this request
//	200  already placed by an earlier attempt
//	409  out of stock
//	202  pending: an earlier attempt's decrement is unknown, so this one cannot
//	     say whether the order went through and must not try again
//	503  nothing happened that a retry would repeat; ask again
func (s *shop) checkout(w http.ResponseWriter, r *http.Request) {
	order, item := r.PathValue("order"), r.URL.Query().Get("item")
	if item == "" {
		http.Error(w, "item is required", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	created, err := s.orders.SetIfAbsent(ctx, order, []byte(statePending+" "+item), 0)
	switch classify(err) {
	case refused:
		fail(w, err)
		return
	case unknown:
		// The claim may or may not exist. A retry finds out: if it was
		// stored, the retry sees a pending order and answers 202.
		fail(w, err)
		return
	}
	if !created {
		s.existingOrder(w, r, order)
		return
	}

	left, err := s.stock.Increment(ctx, item, -1, 0)
	switch classify(err) {
	case refused:
		// Nothing was decremented, so the claim can go and a retry can
		// start over. If the delete itself is not known to have happened,
		// the order stays pending: the stock is right, the order is lost
		// to the client as "pending", and the checker counts it uncertain.
		if classify(s.orders.Delete(ctx, order)) == applied {
			fail(w, err)
			return
		}
		pending(w)
		return
	case unknown:
		logf("checkout %s: decrement unknown, order stays pending: %v", order, err)
		pending(w)
		return
	}

	if left < -InitialStock { // stock is stored as a delta from InitialStock
		if classify(s.retrySet(ctx, func() error {
			_, err := s.stock.Increment(ctx, item, 1, 0)
			return err
		}, false)) != applied {
			pending(w) // the decrement happened; its undo is unknown
			return
		}
		if s.retrySet(ctx, func() error {
			return s.orders.Set(ctx, order, []byte(stateRefused+" "+item), 0)
		}, true) != nil {
			pending(w)
			return
		}
		http.Error(w, "out of stock", http.StatusConflict)
		return
	}

	// The decrement happened. Set is idempotent, so marking the order placed
	// can be retried until it is known to have landed.
	if err := s.retrySet(ctx, func() error {
		return s.orders.Set(ctx, order, []byte(statePlaced+" "+item), 0)
	}, true); err != nil {
		logf("checkout %s: decremented, but not yet marked placed: %v", order, err)
		pending(w)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// existingOrder answers a retry by the order's recorded state.
func (s *shop) existingOrder(w http.ResponseWriter, r *http.Request, order string) {
	v, err := s.orders.Get(r.Context(), order)
	if err != nil {
		fail(w, err)
		return
	}
	state, _, _ := strings.Cut(string(v), " ")
	switch state {
	case statePlaced:
		w.WriteHeader(http.StatusOK)
	case stateRefused:
		http.Error(w, "out of stock", http.StatusConflict)
	default:
		pending(w)
	}
}

func pending(w http.ResponseWriter) {
	http.Error(w, "order pending: outcome unknown", http.StatusAccepted)
}

// retrySet runs write until it is known to have happened, for writes that are
// safe to repeat. idempotent false runs it once more only after a refusal,
// which applied nothing. It gives up after a few attempts or with the
// request's context, and returns the last error.
func (s *shop) retrySet(ctx context.Context, write func() error, idempotent bool) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		err = write()
		switch classify(err) {
		case applied:
			return nil
		case unknown:
			if !idempotent {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Duration(50*(attempt+1)) * time.Millisecond):
		}
	}
	return err
}

func (s *shop) getOrder(w http.ResponseWriter, r *http.Request) {
	v, err := s.orders.Get(r.Context(), r.PathValue("order"))
	if err != nil {
		fail(w, err)
		return
	}
	_, _ = w.Write(v)
}

func (s *shop) getStock(w http.ResponseWriter, r *http.Request) {
	v, err := s.stock.Get(r.Context(), r.PathValue("item"))
	delta := 0
	switch {
	case errors.Is(err, peercache.ErrNotFound):
	case err != nil:
		fail(w, err)
		return
	default:
		delta, _ = strconv.Atoi(string(v))
	}
	_, _ = w.Write([]byte(strconv.Itoa(InitialStock + delta)))
}

// fail maps library errors to HTTP statuses a load generator can tell apart:
// a miss, an overload, an "ask again", and everything else.
func fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, peercache.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, peercache.ErrGroupOverloaded):
		http.Error(w, err.Error(), http.StatusTooManyRequests)
	case errors.Is(err, peercache.ErrNotOwner), errors.Is(err, peercache.ErrActivationPending),
		errors.Is(err, peercache.ErrStaleEpoch), errors.Is(err, peercache.ErrNodeUnreachable):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, context.DeadlineExceeded):
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
	default:
		logf("500: %v", err)
		http.Error(w, strings.TrimSpace(err.Error()), http.StatusInternalServerError)
	}
}

// logf writes one timestamped line to stderr, which is where a pod's log is
// collected. The demo is an application, so it logs as one would; the library
// it embeds takes a Logger and logs nothing by itself.
// stderrLogger is the library's Logger, written where the shop's own lines go.
type stderrLogger struct{}

func (stderrLogger) Debug(string, ...any)        {}
func (stderrLogger) Info(msg string, kv ...any)  { logf("peercache: %s %v", msg, kv) }
func (stderrLogger) Warn(msg string, kv ...any)  { logf("peercache WARN: %s %v", msg, kv) }
func (stderrLogger) Error(msg string, kv ...any) { logf("peercache ERROR: %s %v", msg, kv) }

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "%s "+format+"\n", append([]any{time.Now().UTC().Format(time.RFC3339Nano)}, args...)...)
}

// fatal logs v and exits non-zero.
func fatal(v ...any) {
	logf("%s", fmt.Sprint(v...))
	os.Exit(1)
}
