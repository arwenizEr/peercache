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
//	POST /checkout/{order}?item=I  idempotent order, stock by Increment
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
	"log"
	"net"
	"net/http"
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
		log.Fatal(err)
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
	)
	if err != nil {
		return err
	}
	go joinLoop(gossip, cfg)
	go catchUpLoop(cache)

	store, err := blobStore(cfg)
	if err != nil {
		return err
	}
	s, err := newShop(cache, store)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	s.routes(mux)
	mux.Handle("GET /metrics", cache.MetricsHandler())
	mux.Handle("/debug/peercache/", http.StripPrefix("/debug/peercache", cache.DebugHandler()))
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
	log.Printf("shop %s serving on %s, peers on %s", cfg.nodeID, cfg.httpAddr, self.Addr)

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
		log.Printf("drain: %v", err)
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
	for {
		time.Sleep(2 * time.Second)
		st := cache.Stats()
		r := cache.Ready()
		if !r.Joined || !r.Settled || uint64(st.Epoch) == done {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		err := cache.CatchUp(ctx)
		cancel()
		if err != nil {
			log.Printf("catch-up incomplete, will retry: %v", err)
			continue
		}
		done = uint64(st.Epoch)
	}
}

// joinLoop resolves the headless service and joins whoever answers, until
// this node sees someone besides itself.
func joinLoop(g *membership.Gossip, cfg config) {
	if explicit := env("SEEDS", ""); explicit != "" {
		for g.Join(strings.Split(explicit, ",")) != nil {
			time.Sleep(time.Second)
		}
		return
	}
	if cfg.seedDNS == "" {
		return
	}
	for {
		ips, err := net.LookupHost(cfg.seedDNS)
		var seeds []string
		for _, ip := range ips {
			if ip != cfg.podIP {
				seeds = append(seeds, net.JoinHostPort(ip, "7946"))
			}
		}
		if err == nil && len(seeds) > 0 {
			if err := g.Join(seeds); err == nil {
				return
			}
		}
		time.Sleep(2 * time.Second)
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
	return pcs3.New(client, cfg.s3Bucket, "shop"), nil
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
		if err != nil {
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

// checkout places one order for one unit of item. The order ID is the client's
// idempotency key: a retry finds the order and decrements nothing. There is no
// transaction across the order and the stock key — by design there are none —
// so a process dying between the two leaves an order without a decrement; the
// checker counts that application window separately from library guarantees.
func (s *shop) checkout(w http.ResponseWriter, r *http.Request) {
	order, item := r.PathValue("order"), r.URL.Query().Get("item")
	if item == "" {
		http.Error(w, "item is required", http.StatusBadRequest)
		return
	}
	created, err := s.orders.SetIfAbsent(r.Context(), order, []byte(item), 0)
	if err != nil {
		fail(w, err)
		return
	}
	if !created {
		w.WriteHeader(http.StatusOK) // a retry: already placed
		return
	}
	left, err := s.stock.Increment(r.Context(), item, -1, 0)
	if err != nil {
		_ = s.orders.Delete(r.Context(), order)
		fail(w, err)
		return
	}
	if left < -InitialStock { // stock is stored as a delta from InitialStock
		_, _ = s.stock.Increment(r.Context(), item, 1, 0)
		_ = s.orders.Delete(r.Context(), order)
		http.Error(w, "out of stock", http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusCreated)
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
		log.Printf("500: %v", err)
		http.Error(w, strings.TrimSpace(err.Error()), http.StatusInternalServerError)
	}
}
