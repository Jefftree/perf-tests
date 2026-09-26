package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

const (
	protobufContentType = "application/vnd.kubernetes.protobuf"
	smpContentType      = "application/strategic-merge-patch+json"
	kubeletUserAgent    = "kubelet/v1.36.1 (linux/amd64) kubernetes/vibe-kubemark"
	kubeProxyUserAgent  = "kube-proxy/v1.36.1 (linux/amd64) kubernetes/vibe-kubemark"
)

var (
	protoSerializer = protobuf.NewSerializer(scheme.Scheme, scheme.Scheme)
	watchBufPool    = sync.Pool{
		New: func() any {
			b := make([]byte, 16384)
			return &b
		},
	}
	defaultNodeImages = buildDefaultNodeImages(25)
)

func buildDefaultNodeImages(count int) []corev1.ContainerImage {
	imgs := make([]corev1.ContainerImage, count)
	imgs[0] = corev1.ContainerImage{
		Names:     []string{"registry.k8s.io/pause:3.9", "registry.k8s.io/pause@sha256:7031c1b283388d2c2e09b57badb803c05ebed362dc88d84b480cc47f72a21097"},
		SizeBytes: 321520,
	}
	imgs[1] = corev1.ContainerImage{
		Names:     []string{"registry.k8s.io/kube-proxy:v1.36.1", "registry.k8s.io/kube-proxy@sha256:a1b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef0123456789abcdef0"},
		SizeBytes: 92451840,
	}
	for i := 2; i < count; i++ {
		imgs[i] = corev1.ContainerImage{
			Names: []string{
				fmt.Sprintf("registry.k8s.io/e2e-test-images/agnhost:2.%d", i),
				fmt.Sprintf("registry.k8s.io/e2e-test-images/agnhost@sha256:%064x", i+1000),
			},
			SizeBytes: int64(25000000 + i*1048576),
		}
	}
	return imgs
}

type Config struct {
	Kubeconfig              string
	APIServerURL            string
	TokensFile              string
	NumNodes                int
	NodePrefix              string
	RegisterConcurrency     int
	PodWorkers              int
	MaxInflightPodMutations int
	PodStatusStages         int
	LeaseInterval           time.Duration
	NodeStatusInterval      time.Duration
	PodStartupDelay         time.Duration
	PodStartupJitter        time.Duration
	JobCompleteDelay        time.Duration
	EnableWatches           bool
	ExtraWatches            bool
	EmitPodEvents           bool
	SpreadLoopbackIPs       bool
	BindPVCs                bool
	CleanupOnExit           bool
}

type Stats struct {
	ActiveWatches   atomic.Int64
	WatchReconnects atomic.Uint64
	WatchBytes      atomic.Uint64
	LeasePUTs       atomic.Uint64
	LeaseErrors     atomic.Uint64
	NodePATCHes     atomic.Uint64
	PodPATCHes      atomic.Uint64
	PodDELETEs      atomic.Uint64
	EventPOSTs      atomic.Uint64
	PodAuthzRetries atomic.Uint64
	PVCsBound       atomic.Uint64
}

type SimNode struct {
	Index           int
	Name            string
	UID             types.UID
	InternalIP      string
	Zone            string
	Token           string
	KubeletClient   *http.Client
	KubeProxyClient *http.Client
	LeaseMu         sync.Mutex
	Lease           *coordinationv1.Lease
	TransitionTime  metav1.Time
	Capacity        corev1.ResourceList
}

type authRoundTripper struct {
	base      http.RoundTripper
	token     string
	userAgent string
}

func (a *authRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req2 := req.Clone(req.Context())
	if a.token != "" {
		req2.Header.Set("Authorization", "Bearer "+a.token)
	}
	if req2.Header.Get("User-Agent") == "" {
		req2.Header.Set("User-Agent", a.userAgent)
	}
	return a.base.RoundTrip(req2)
}

type Simulator struct {
	cfg          Config
	baseURL      string
	isLoopback   bool
	adminClient  *kubernetes.Clientset
	adminHTTP    *http.Client
	tlsConfig    *tls.Config
	nodesByName  map[string]*SimNode
	nodes        []*SimNode
	stats        Stats
	podMutateSem chan struct{}

	latestPodRV           atomic.Value // string
	latestNodeRV          atomic.Value // string
	latestServiceRV       atomic.Value // string
	latestEndpointSliceRV atomic.Value // string

	podWorkCh chan podWorkItem
	seenPods  sync.Map // types.UID -> podActuationState
}

type podActuationState int

const (
	podStateNone podActuationState = iota
	podStateQueuedRunning
	podStateRunning
	podStateQueuedSucceeded
	podStateSucceeded
	podStateQueuedDeleted
	podStateDeleted
)

type podWorkItem struct {
	pod      *corev1.Pod
	isDelete bool
	isJobPod bool
}

func main() {
	var cfg Config
	flag.StringVar(&cfg.Kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "Path to admin kubeconfig")
	flag.StringVar(&cfg.APIServerURL, "apiserver-url", "", "Optional override for kube-apiserver URL (e.g. https://172.18.0.2:6443)")
	flag.StringVar(&cfg.TokensFile, "tokens-file", "", "Optional CSV token-auth-file (token,system:node:<name>,uid,\"system:nodes\") for per-node APF & NodeAuthorizer identity")
	flag.IntVar(&cfg.NumNodes, "nodes", 5000, "Number of simulated nodes")
	flag.StringVar(&cfg.NodePrefix, "node-prefix", "vibe-node-", "Node name prefix")
	flag.IntVar(&cfg.RegisterConcurrency, "register-concurrency", 128, "Concurrency for initial node & lease registration")
	flag.IntVar(&cfg.PodWorkers, "pod-workers", 512, "Concurrent workers actuating Pod Pending->Running/Succeeded and Terminating->Delete")
	flag.IntVar(&cfg.MaxInflightPodMutations, "max-inflight-pod-mutations", 96, "Max concurrent in-flight system:node pod status PATCH / DELETE requests")
	flag.IntVar(&cfg.PodStatusStages, "pod-status-stages", 3, "Number of PodStatus PATCH transitions per pod lifecycle (1=Running+Ready only, 3=ContainerCreating->StartedNotReady->RunningReady + Terminating patch)")
	flag.DurationVar(&cfg.LeaseInterval, "lease-interval", 10*time.Second, "Per-node Lease heartbeat interval")
	flag.DurationVar(&cfg.NodeStatusInterval, "node-status-interval", 90*time.Second, "Per-node NodeStatus PATCH interval (updates LastHeartbeatTime + 25 container images = ~4.9KB Node fanout)")
	flag.DurationVar(&cfg.PodStartupDelay, "pod-startup-delay", 150*time.Millisecond, "Minimum kubelet delay after PodScheduled before patching PodStatus (prevents NodeAuthorizer informer lag 403s)")
	flag.DurationVar(&cfg.PodStartupJitter, "pod-startup-jitter", 350*time.Millisecond, "Random kubelet container startup jitter added to pod-startup-delay")
	flag.DurationVar(&cfg.JobCompleteDelay, "job-complete-delay", 100*time.Millisecond, "Delay before transitioning non-pause Job pods from Running to Succeeded")
	flag.BoolVar(&cfg.EnableWatches, "enable-watches", true, "Open independent HTTP/2 connections per node (kubelet + kube-proxy) with per-node watches")
	flag.BoolVar(&cfg.ExtraWatches, "extra-watches", true, "Open full 11-watch per-node profile (2x services, 2x nodes, pods, endpointslices, configmaps, csidrivers, csinodes, runtimeclasses, servicecidrs = 55k watches)")
	flag.BoolVar(&cfg.EmitPodEvents, "emit-pod-events", true, "Emit Pulled/Started corev1.Event objects on pod startup to exercise split etcd-events")
	flag.BoolVar(&cfg.SpreadLoopbackIPs, "spread-loopback-ips", true, "When dialing 127.0.0.1, bind source IPs across 127.0.1.1..127.0.25.250 to avoid ephemeral port exhaustion")
	flag.BoolVar(&cfg.BindPVCs, "bind-pvcs", true, "Automatically provision and bind Pending PVCs so StatefulSets schedule immediately")
	flag.BoolVar(&cfg.CleanupOnExit, "cleanup-on-exit", false, "Delete simulated nodes on SIGINT/SIGTERM")
	klog.InitFlags(nil)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sim, err := NewSimulator(cfg)
	if err != nil {
		klog.Fatalf("Failed to initialize simulator: %v", err)
	}

	if err := sim.Run(ctx); err != nil && ctx.Err() == nil {
		klog.Fatalf("Simulator failed: %v", err)
	}
}

func NewSimulator(cfg Config) (*Simulator, error) {
	restCfg, err := clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig %q: %w", cfg.Kubeconfig, err)
	}
	if cfg.APIServerURL != "" {
		restCfg.Host = cfg.APIServerURL
	}
	restCfg.QPS = 2000
	restCfg.Burst = 4000
	restCfg.ContentType = protobufContentType
	restCfg.AcceptContentTypes = protobufContentType + ",application/json"

	adminClient, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("creating admin clientset: %w", err)
	}
	adminHTTP, err := rest.HTTPClientFor(restCfg)
	if err != nil {
		return nil, fmt.Errorf("creating admin HTTP client: %w", err)
	}

	u, err := url.Parse(restCfg.Host)
	if err != nil {
		return nil, fmt.Errorf("parsing host %q: %w", restCfg.Host, err)
	}
	hostOnly := u.Hostname()
	isLoopback := hostOnly == "127.0.0.1" || hostOnly == "localhost"

	tokensByNode := make(map[string]string, cfg.NumNodes)
	if cfg.TokensFile != "" {
		data, err := os.ReadFile(cfg.TokensFile)
		if err != nil {
			return nil, fmt.Errorf("reading tokens file %q: %w", cfg.TokensFile, err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			parts := strings.Split(line, ",")
			if len(parts) >= 2 {
				token := parts[0]
				user := strings.TrimPrefix(parts[1], "system:node:")
				tokensByNode[user] = token
			}
		}
		klog.Infof("Loaded %d node tokens from %s", len(tokensByNode), cfg.TokensFile)
	}

	tlsCfg, err := rest.TLSConfigFor(restCfg)
	if err != nil {
		return nil, fmt.Errorf("building TLS config: %w", err)
	}
	if tlsCfg == nil {
		tlsCfg = &tls.Config{InsecureSkipVerify: true}
	}
	tlsCfg.NextProtos = []string{"h2", "http/1.1"}
	tlsCfg.ClientSessionCache = tls.NewLRUClientSessionCache(256)

	maxInflight := cfg.MaxInflightPodMutations
	if maxInflight <= 0 {
		maxInflight = 40
	}

	sim := &Simulator{
		cfg:          cfg,
		baseURL:      strings.TrimRight(restCfg.Host, "/"),
		isLoopback:   isLoopback,
		adminClient:  adminClient,
		adminHTTP:    adminHTTP,
		tlsConfig:    tlsCfg,
		nodesByName:  make(map[string]*SimNode, cfg.NumNodes),
		nodes:        make([]*SimNode, cfg.NumNodes),
		podWorkCh:    make(chan podWorkItem, 131072),
		podMutateSem: make(chan struct{}, maxInflight),
	}
	sim.latestPodRV.Store("0")
	sim.latestNodeRV.Store("0")
	sim.latestServiceRV.Store("0")
	sim.latestEndpointSliceRV.Store("0")

	capList := corev1.ResourceList{
		corev1.ResourceCPU:              resource.MustParse("4"),
		corev1.ResourceMemory:           resource.MustParse("16Gi"),
		corev1.ResourcePods:             resource.MustParse("110"),
		corev1.ResourceEphemeralStorage: resource.MustParse("100Gi"),
	}

	zones := []string{"us-east1-b", "us-east1-c", "us-east1-d"}
	for i := 0; i < cfg.NumNodes; i++ {
		name := fmt.Sprintf("%s%04d", cfg.NodePrefix, i)
		ip := fmt.Sprintf("10.128.%d.%d", (i/250)+1, (i%250)+2)
		token := tokensByNode[name]
		// Create 2 distinct HTTP/2 clients per node (1 for kubelet, 1 for kube-proxy) = 10,000 HTTP/2 connections at 5k nodes
		kubeletClient := sim.newNodeHTTPClient(i*2, token, restCfg.BearerToken, kubeletUserAgent)
		kubeProxyClient := sim.newNodeHTTPClient(i*2+1, token, restCfg.BearerToken, kubeProxyUserAgent)
		n := &SimNode{
			Index:           i,
			Name:            name,
			InternalIP:      ip,
			Zone:            zones[i%len(zones)],
			Token:           token,
			KubeletClient:   kubeletClient,
			KubeProxyClient: kubeProxyClient,
			Capacity:        capList,
		}
		sim.nodes[i] = n
		sim.nodesByName[name] = n
	}

	return sim, nil
}

func (s *Simulator) newNodeHTTPClient(connIdx int, nodeToken, fallbackBearer, userAgent string) *http.Client {
	dialer := &net.Dialer{
		Timeout:   15 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	if s.isLoopback && s.cfg.SpreadLoopbackIPs {
		octet2 := byte(1 + (connIdx / 200))
		octet3 := byte(1 + (connIdx % 200))
		dialer.LocalAddr = &net.TCPAddr{IP: net.IPv4(127, 0, octet2, octet3)}
	}

	nodeTLS := s.tlsConfig.Clone()
	if s.cfg.TokensFile != "" && nodeToken != "" {
		nodeTLS.Certificates = nil
		nodeTLS.GetClientCertificate = nil
	}

	tr := &http.Transport{
		DialContext:           dialer.DialContext,
		TLSClientConfig:       nodeTLS,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          2,
		MaxIdleConnsPerHost:   2,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ReadBufferSize:        8192,
		WriteBufferSize:       8192,
	}

	tok := nodeToken
	if tok == "" {
		tok = fallbackBearer
	}
	return &http.Client{
		Transport: &authRoundTripper{
			base:      tr,
			token:     tok,
			userAgent: userAgent,
		},
	}
}

func (s *Simulator) Run(ctx context.Context) error {
	start := time.Now()
	klog.Infof("Registering %d simulated nodes (prefix=%q, concurrency=%d, perNodeTokens=%v, maxInflightPodMutations=%d)...",
		s.cfg.NumNodes, s.cfg.NodePrefix, s.cfg.RegisterConcurrency, s.cfg.TokensFile != "", cap(s.podMutateSem))

	if err := s.registerAllNodesAndLeases(ctx); err != nil {
		return err
	}
	klog.Infof("Registered %d nodes + leases in %v", s.cfg.NumNodes, time.Since(start).Round(time.Millisecond))

	if err := s.seedInitialResourceVersions(ctx); err != nil {
		klog.Warningf("Non-fatal error seeding initial resourceVersions: %v", err)
	}

	for w := 0; w < s.cfg.PodWorkers; w++ {
		go s.podActuatorWorker(ctx)
	}
	go s.watchClusterPods(ctx)

	if s.cfg.BindPVCs {
		go s.watchAndBindPVCs(ctx)
	}

	for _, n := range s.nodes {
		go s.runNodeLeaseLoop(ctx, n)
		if s.cfg.NodeStatusInterval > 0 {
			go s.runNodeStatusLoop(ctx, n)
		}
		if s.cfg.EnableWatches {
			s.startNodeWatches(ctx, n)
		}
	}

	go s.reportTelemetryLoop(ctx)

	<-ctx.Done()
	if s.cfg.CleanupOnExit {
		s.cleanupNodes()
	}
	return nil
}

func (s *Simulator) registerAllNodesAndLeases(ctx context.Context) error {
	sem := make(chan struct{}, s.cfg.RegisterConcurrency)
	var wg sync.WaitGroup
	var firstErr atomic.Value

	for _, n := range s.nodes {
		wg.Add(1)
		sem <- struct{}{}
		go func(node *SimNode) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := s.registerSingleNode(ctx, node); err != nil {
				firstErr.CompareAndSwap(nil, err)
			}
		}(n)
	}
	wg.Wait()
	if v := firstErr.Load(); v != nil {
		return v.(error)
	}
	return nil
}

func (s *Simulator) buildNodeStatusPayload(n *SimNode, heartbeatTime metav1.Time) []byte {
	trans := n.TransitionTime
	if trans.IsZero() {
		trans = heartbeatTime
	}
	status := corev1.NodeStatus{
		Capacity:    n.Capacity,
		Allocatable: n.Capacity,
		Phase:       corev1.NodeRunning,
		Addresses: []corev1.NodeAddress{
			{Type: corev1.NodeInternalIP, Address: n.InternalIP},
			{Type: corev1.NodeHostName, Address: n.Name},
		},
		Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: corev1.ConditionTrue, LastHeartbeatTime: heartbeatTime, LastTransitionTime: trans, Reason: "KubeletReady", Message: "kubelet is posting ready status"},
			{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse, LastHeartbeatTime: heartbeatTime, LastTransitionTime: trans, Reason: "KubeletHasSufficientMemory", Message: "kubelet has sufficient memory available"},
			{Type: corev1.NodeDiskPressure, Status: corev1.ConditionFalse, LastHeartbeatTime: heartbeatTime, LastTransitionTime: trans, Reason: "KubeletHasNoDiskPressure", Message: "kubelet has no disk pressure"},
			{Type: corev1.NodePIDPressure, Status: corev1.ConditionFalse, LastHeartbeatTime: heartbeatTime, LastTransitionTime: trans, Reason: "KubeletHasSufficientPID", Message: "kubelet has sufficient PID available"},
			{Type: corev1.NodeNetworkUnavailable, Status: corev1.ConditionFalse, LastHeartbeatTime: heartbeatTime, LastTransitionTime: trans, Reason: "RouteCreated", Message: "RouteController created a route"},
		},
		NodeInfo: corev1.NodeSystemInfo{
			MachineID:               fmt.Sprintf("ec2a14f9081240a19834%012d", n.Index),
			SystemUUID:              fmt.Sprintf("ec2a14f9-0812-40a1-9834-%012d", n.Index),
			BootID:                  fmt.Sprintf("8a7b6c5d-4e3f-2a1b-0c9d-%012d", n.Index),
			KubeletVersion:          "v1.36.1",
			KubeProxyVersion:        "v1.36.1",
			ContainerRuntimeVersion: "containerd://2.3.0",
			OperatingSystem:         "linux",
			Architecture:            "amd64",
			OSImage:                 "Ubuntu 24.04.2 LTS",
			KernelVersion:           "6.8.0-1021-gcp",
		},
		Images: defaultNodeImages,
	}
	raw, _ := json.Marshal(map[string]any{"status": status})
	return raw
}

func (s *Simulator) registerSingleNode(ctx context.Context, n *SimNode) error {
	now := metav1.Now()
	n.TransitionTime = now
	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: n.Name,
			Labels: map[string]string{
				"beta.kubernetes.io/arch":        "amd64",
				"beta.kubernetes.io/os":          "linux",
				"kubernetes.io/hostname":         n.Name,
				"kubernetes.io/os":               "linux",
				"kubernetes.io/arch":             "amd64",
				"kubernetes.io/role":             "node",
				"node-role.kubernetes.io/node":   "",
				"topology.kubernetes.io/region":  "us-east1",
				"topology.kubernetes.io/zone":    n.Zone,
				"node.kubernetes.io/instance-type": "e2-medium",
				"vibe.k8s.io/simulated":          "true",
			},
		},
		Spec: corev1.NodeSpec{
			PodCIDR:    fmt.Sprintf("10.%d.%d.0/24", 64+(n.Index/256), n.Index%256),
			PodCIDRs:   []string{fmt.Sprintf("10.%d.%d.0/24", 64+(n.Index/256), n.Index%256)},
			ProviderID: fmt.Sprintf("gce://vibe-5k/%s/%s", n.Zone, n.Name),
		},
	}

	created, err := s.adminClient.CoreV1().Nodes().Create(ctx, nodeObj, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		created, err = s.adminClient.CoreV1().Nodes().Get(ctx, n.Name, metav1.GetOptions{})
	}
	if err != nil {
		return fmt.Errorf("creating node %s: %w", n.Name, err)
	}
	n.UID = created.UID

	if err := s.patchNodeStatus(ctx, n); err != nil {
		return fmt.Errorf("patching node status %s: %w", n.Name, err)
	}

	renewTime := metav1.NowMicro()
	leaseDuration := int32(40)
	leaseObj := &coordinationv1.Lease{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "coordination.k8s.io/v1",
			Kind:       "Lease",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      n.Name,
			Namespace: corev1.NamespaceNodeLease,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Node",
					Name:       n.Name,
					UID:        n.UID,
				},
			},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &n.Name,
			LeaseDurationSeconds: &leaseDuration,
			RenewTime:            &renewTime,
		},
	}

	gotLease, err := s.adminClient.CoordinationV1().Leases(corev1.NamespaceNodeLease).Create(ctx, leaseObj, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		gotLease, err = s.adminClient.CoordinationV1().Leases(corev1.NamespaceNodeLease).Get(ctx, n.Name, metav1.GetOptions{})
	}
	if err != nil {
		return fmt.Errorf("creating lease %s: %w", n.Name, err)
	}
	gotLease.TypeMeta = leaseObj.TypeMeta
	n.Lease = gotLease
	return nil
}

func (s *Simulator) seedInitialResourceVersions(ctx context.Context) error {
	s.refreshResourceVersionsOnce(ctx)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.refreshResourceVersionsOnce(ctx)
			}
		}
	}()
	return nil
}

func (s *Simulator) refreshResourceVersionsOnce(ctx context.Context) {
	if pods, err := s.adminClient.CoreV1().Pods("kube-system").List(ctx, metav1.ListOptions{Limit: 1}); err == nil && pods.ResourceVersion != "" {
		s.latestPodRV.Store(pods.ResourceVersion)
	}
	if nodes, err := s.adminClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 1}); err == nil && nodes.ResourceVersion != "" {
		s.latestNodeRV.Store(nodes.ResourceVersion)
	}
	if svcs, err := s.adminClient.CoreV1().Services("default").List(ctx, metav1.ListOptions{Limit: 1}); err == nil && svcs.ResourceVersion != "" {
		s.latestServiceRV.Store(svcs.ResourceVersion)
	}
	if slices, err := s.adminClient.DiscoveryV1().EndpointSlices("default").List(ctx, metav1.ListOptions{Limit: 1}); err == nil && slices.ResourceVersion != "" {
		s.latestEndpointSliceRV.Store(slices.ResourceVersion)
	}
}

func (s *Simulator) runNodeLeaseLoop(ctx context.Context, n *SimNode) {
	initialDelay := time.Duration(int64(n.Index) * int64(s.cfg.LeaseInterval) / int64(max(1, s.cfg.NumNodes)))
	select {
	case <-ctx.Done():
		return
	case <-time.After(initialDelay):
	}

	ticker := time.NewTicker(s.cfg.LeaseInterval)
	defer ticker.Stop()

	var buf bytes.Buffer
	for {
		if err := s.putNodeLeaseProtobuf(ctx, n, &buf); err != nil {
			s.stats.LeaseErrors.Add(1)
		} else {
			s.stats.LeasePUTs.Add(1)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Simulator) putNodeLeaseProtobuf(ctx context.Context, n *SimNode, buf *bytes.Buffer) error {
	n.LeaseMu.Lock()
	defer n.LeaseMu.Unlock()

	now := metav1.NowMicro()
	n.Lease.Spec.RenewTime = &now
	buf.Reset()
	if err := protoSerializer.Encode(n.Lease, buf); err != nil {
		return err
	}

	leaseURL := fmt.Sprintf("%s/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases/%s?timeout=10s", s.baseURL, n.Name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, leaseURL, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", protobufContentType)
	req.Header.Set("Accept", protobufContentType)

	resp, err := n.KubeletClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusConflict {
		if fresh, getErr := s.adminClient.CoordinationV1().Leases(corev1.NamespaceNodeLease).Get(ctx, n.Name, metav1.GetOptions{}); getErr == nil {
			n.Lease.ResourceVersion = fresh.ResourceVersion
			n.Lease.UID = fresh.UID
		}
		return fmt.Errorf("lease conflict 409")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("lease PUT status %d", resp.StatusCode)
	}

	var updated coordinationv1.Lease
	if _, _, decErr := protoSerializer.Decode(body, nil, &updated); decErr == nil && updated.ResourceVersion != "" {
		n.Lease.ResourceVersion = updated.ResourceVersion
	}
	return nil
}

func (s *Simulator) runNodeStatusLoop(ctx context.Context, n *SimNode) {
	initialDelay := time.Duration(int64(n.Index) * int64(s.cfg.NodeStatusInterval) / int64(max(1, s.cfg.NumNodes)))
	select {
	case <-ctx.Done():
		return
	case <-time.After(initialDelay):
	}

	ticker := time.NewTicker(s.cfg.NodeStatusInterval)
	defer ticker.Stop()
	for {
		if err := s.patchNodeStatus(ctx, n); err == nil {
			s.stats.NodePATCHes.Add(1)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Simulator) patchNodeStatus(ctx context.Context, n *SimNode) error {
	payload := s.buildNodeStatusPayload(n, metav1.Now())
	u := fmt.Sprintf("%s/api/v1/nodes/%s/status?timeout=10s", s.baseURL, n.Name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, u, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", smpContentType)
	req.Header.Set("Accept", protobufContentType)

	resp, err := n.KubeletClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("node status PATCH %s returned %d", n.Name, resp.StatusCode)
	}
	return nil
}

func (s *Simulator) startNodeWatches(ctx context.Context, n *SimNode) {
	// Stagger watch connection establishment across first 6 seconds
	stagger := time.Duration(int64(n.Index) * int64(6*time.Second) / int64(max(1, s.cfg.NumNodes)))

	// --- Kubelet connection watches (n.KubeletClient) ---
	// 1. Kubelet Pod watch: fieldSelector=spec.nodeName=<nodeName>
	go s.runWatchDrainLoop(ctx, n.KubeletClient, stagger, kubeletUserAgent, func() string {
		return fmt.Sprintf("%s/api/v1/pods?watch=true&fieldSelector=spec.nodeName%%3D%s&allowWatchBookmarks=true&resourceVersion=%s",
			s.baseURL, n.Name, s.latestPodRV.Load().(string))
	})

	// 2. Kubelet Node watch: fieldSelector=metadata.name=<nodeName>
	go s.runWatchDrainLoop(ctx, n.KubeletClient, stagger+40*time.Millisecond, kubeletUserAgent, func() string {
		return fmt.Sprintf("%s/api/v1/nodes?watch=true&fieldSelector=metadata.name%%3D%s&allowWatchBookmarks=true&resourceVersion=%s",
			s.baseURL, n.Name, s.latestNodeRV.Load().(string))
	})

	// --- Kube-Proxy connection watches (n.KubeProxyClient) ---
	// 3. Kube-Proxy Service watch: labelSelector=!service.kubernetes.io/service-proxy-name
	go s.runWatchDrainLoop(ctx, n.KubeProxyClient, stagger+80*time.Millisecond, kubeProxyUserAgent, func() string {
		return fmt.Sprintf("%s/api/v1/services?watch=true&labelSelector=%%21service.kubernetes.io%%2Fservice-proxy-name&allowWatchBookmarks=true&resourceVersion=%s",
			s.baseURL, s.latestServiceRV.Load().(string))
	})

	// 4. Kube-Proxy EndpointSlice watch: labelSelector=!service.kubernetes.io/headless
	go s.runWatchDrainLoop(ctx, n.KubeProxyClient, stagger+120*time.Millisecond, kubeProxyUserAgent, func() string {
		return fmt.Sprintf("%s/apis/discovery.k8s.io/v1/endpointslices?watch=true&labelSelector=%%21service.kubernetes.io%%2Fheadless&allowWatchBookmarks=true&resourceVersion=%s",
			s.baseURL, s.latestEndpointSliceRV.Load().(string))
	})

	if s.cfg.ExtraWatches {
		// 5. Kubelet Service watch (for pod service env-var injection; makes 10,000 total services watches matching CI's 10,321)
		go s.runWatchDrainLoop(ctx, n.KubeletClient, stagger+160*time.Millisecond, kubeletUserAgent, func() string {
			return fmt.Sprintf("%s/api/v1/services?watch=true&allowWatchBookmarks=true&resourceVersion=%s",
				s.baseURL, s.latestServiceRV.Load().(string))
		})

		// 6. Kube-Proxy Node watch (watches own Node for topology/zone labels; makes 10,000 total nodes watches matching CI's 10,004)
		go s.runWatchDrainLoop(ctx, n.KubeProxyClient, stagger+200*time.Millisecond, kubeProxyUserAgent, func() string {
			return fmt.Sprintf("%s/api/v1/nodes?watch=true&fieldSelector=metadata.name%%3D%s&allowWatchBookmarks=true&resourceVersion=%s",
				s.baseURL, n.Name, s.latestNodeRV.Load().(string))
		})

		// 7. Kubelet CSIDriver watch (5,000 watches matching CI's 5,005)
		go s.runWatchDrainLoop(ctx, n.KubeletClient, stagger+240*time.Millisecond, kubeletUserAgent, func() string {
			return fmt.Sprintf("%s/apis/storage.k8s.io/v1/csidrivers?watch=true&allowWatchBookmarks=true&resourceVersion=0", s.baseURL)
		})

		// 8. Kubelet CSINode watch (5,000 watches)
		go s.runWatchDrainLoop(ctx, n.KubeletClient, stagger+280*time.Millisecond, kubeletUserAgent, func() string {
			return fmt.Sprintf("%s/apis/storage.k8s.io/v1/csinodes?watch=true&fieldSelector=metadata.name%%3D%s&allowWatchBookmarks=true&resourceVersion=0",
				s.baseURL, n.Name)
		})

		// 9. Kubelet RuntimeClass watch (5,000 watches matching CI's 5,004)
		go s.runWatchDrainLoop(ctx, n.KubeletClient, stagger+320*time.Millisecond, kubeletUserAgent, func() string {
			return fmt.Sprintf("%s/apis/node.k8s.io/v1/runtimeclasses?watch=true&allowWatchBookmarks=true&resourceVersion=0", s.baseURL)
		})

		// 10. Kube-Proxy ServiceCIDR watch (5,000 watches matching CI's 5,004)
		go s.runWatchDrainLoop(ctx, n.KubeProxyClient, stagger+360*time.Millisecond, kubeProxyUserAgent, func() string {
			return fmt.Sprintf("%s/apis/networking.k8s.io/v1/servicecidrs?watch=true&allowWatchBookmarks=true&resourceVersion=0", s.baseURL)
		})

		// 11. Kubelet kube-root-ca.crt ConfigMap watch (5,000 watches matching CI's 5,806)
		go s.runWatchDrainLoop(ctx, n.KubeletClient, stagger+400*time.Millisecond, kubeletUserAgent, func() string {
			return fmt.Sprintf("%s/api/v1/namespaces/kube-system/configmaps?watch=true&fieldSelector=metadata.name%%3Dkube-root-ca.crt&allowWatchBookmarks=true&resourceVersion=0", s.baseURL)
		})
	}
}

func (s *Simulator) runWatchDrainLoop(ctx context.Context, client *http.Client, initialDelay time.Duration, ua string, buildURL func() string) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(initialDelay):
	}

	for ctx.Err() == nil {
		timeoutSec := 7200 + rand.IntN(3600)
		u := fmt.Sprintf("%s&timeoutSeconds=%d", buildURL(), timeoutSec)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return
		}
		req.Header.Set("Accept", protobufContentType)
		req.Header.Set("User-Agent", ua)

		resp, err := client.Do(req)
		if err != nil {
			s.stats.WatchReconnects.Add(1)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(500+rand.IntN(1500)) * time.Millisecond):
				continue
			}
		}

		if resp.StatusCode != http.StatusOK {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			s.stats.WatchReconnects.Add(1)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(1000+rand.IntN(2000)) * time.Millisecond):
				continue
			}
		}

		s.stats.ActiveWatches.Add(1)
		bufPtr := watchBufPool.Get().(*[]byte)
		buf := *bufPtr
		for {
			nr, rerr := resp.Body.Read(buf)
			if nr > 0 {
				s.stats.WatchBytes.Add(uint64(nr))
			}
			if rerr != nil {
				break
			}
		}
		watchBufPool.Put(bufPtr)
		resp.Body.Close()
		s.stats.ActiveWatches.Add(-1)
		s.stats.WatchReconnects.Add(1)
	}
}

func (s *Simulator) watchClusterPods(ctx context.Context) {
	go s.reconcileStragglerPods(ctx)
	for ctx.Err() == nil {
		list, err := s.adminClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{ResourceVersion: "0"})
		if err != nil {
			klog.Warningf("Pod list error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		s.latestPodRV.Store(list.ResourceVersion)
		for i := range list.Items {
			s.inspectPod(&list.Items[i])
		}

		w, err := s.adminClient.CoreV1().Pods("").Watch(ctx, metav1.ListOptions{
			ResourceVersion:     list.ResourceVersion,
			AllowWatchBookmarks: true,
		})
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		for ev := range w.ResultChan() {
			pod, ok := ev.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			if pod.ResourceVersion != "" {
				s.latestPodRV.Store(pod.ResourceVersion)
			}
			if ev.Type == watch.Deleted {
				s.seenPods.Delete(pod.UID)
				continue
			}
			s.inspectPod(pod)
		}
	}
}

func (s *Simulator) reconcileStragglerPods(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			list, err := s.adminClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{ResourceVersion: "0"})
			if err != nil {
				continue
			}
			for i := range list.Items {
				pod := &list.Items[i]
				if pod.Spec.NodeName == "" {
					continue
				}
				s.inspectPod(pod)
			}
		}
	}
}

func (s *Simulator) inspectPod(pod *corev1.Pod) {
	if pod.Spec.NodeName == "" {
		return
	}
	if _, isSimNode := s.nodesByName[pod.Spec.NodeName]; !isSimNode {
		return
	}

	if pod.DeletionTimestamp != nil {
		if prev, ok := s.seenPods.Load(pod.UID); ok {
			st := prev.(podActuationState)
			if st == podStateQueuedDeleted || st == podStateDeleted {
				return
			}
		}
		s.seenPods.Store(pod.UID, podStateQueuedDeleted)
		s.podWorkCh <- podWorkItem{pod: pod, isDelete: true}
		return
	}

	isJobPod := (pod.Spec.RestartPolicy == corev1.RestartPolicyOnFailure || pod.Spec.RestartPolicy == corev1.RestartPolicyNever) &&
		len(pod.Spec.Containers) > 0 && !strings.Contains(pod.Spec.Containers[0].Image, "pause")
	if isJobPod {
		if pod.Status.Phase == corev1.PodSucceeded {
			s.seenPods.Store(pod.UID, podStateSucceeded)
			return
		}
		if prev, ok := s.seenPods.Load(pod.UID); ok {
			st := prev.(podActuationState)
			if st == podStateQueuedSucceeded || st == podStateSucceeded {
				return
			}
		}
		s.seenPods.Store(pod.UID, podStateQueuedSucceeded)
		s.podWorkCh <- podWorkItem{pod: pod, isJobPod: true}
		return
	}

	if pod.Status.Phase == corev1.PodRunning && isPodReady(pod) {
		s.seenPods.Store(pod.UID, podStateRunning)
		return
	}
	if prev, ok := s.seenPods.Load(pod.UID); ok {
		st := prev.(podActuationState)
		if st == podStateQueuedRunning || st == podStateRunning {
			return
		}
	}
	s.seenPods.Store(pod.UID, podStateQueuedRunning)
	s.podWorkCh <- podWorkItem{pod: pod}
}

func isPodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (s *Simulator) isPodTerminating(uid types.UID) bool {
	if prev, ok := s.seenPods.Load(uid); ok {
		st := prev.(podActuationState)
		return st == podStateQueuedDeleted || st == podStateDeleted
	}
	return false
}

func (s *Simulator) podActuatorWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-s.podWorkCh:
			n := s.nodesByName[item.pod.Spec.NodeName]
			if n == nil {
				continue
			}
			if item.isDelete {
				if prev, ok := s.seenPods.Load(item.pod.UID); ok && prev.(podActuationState) == podStateDeleted {
					continue
				}
				if s.cfg.PodStatusStages >= 2 {
					_ = s.patchPodTerminatingAsNode(ctx, n, item.pod)
				}
				if err := s.deleteTerminatingPodAsNode(ctx, n, item.pod); err != nil {
					s.seenPods.Delete(item.pod.UID)
				} else {
					s.seenPods.Store(item.pod.UID, podStateDeleted)
				}
				continue
			}

			if prev, ok := s.seenPods.Load(item.pod.UID); ok {
				st := prev.(podActuationState)
				if (!item.isJobPod && st == podStateRunning) || (item.isJobPod && st == podStateSucceeded) || st == podStateQueuedDeleted || st == podStateDeleted {
					continue
				}
			}

			// Simulate realistic kubelet sandbox + container startup delay (150ms + jitter)
			// This both matches real GCE PodStartupLatency (schedule_to_watch ~400-800ms) and ensures
			// kube-apiserver's internal NodeAuthorizer pod informer has processed the binding.
			delay := s.cfg.PodStartupDelay
			if s.cfg.PodStartupJitter > 0 {
				delay += time.Duration(rand.Int64N(int64(s.cfg.PodStartupJitter)))
			}

			if s.cfg.PodStatusStages >= 3 {
				// Stage 1: Sandbox allocated + ContainerCreating (Pending, Ready=False)
				stage1Delay := max(80*time.Millisecond, delay/3)
				select {
				case <-ctx.Done():
					return
				case <-time.After(stage1Delay):
				}
				if s.isPodTerminating(item.pod.UID) {
					continue
				}
				_ = s.patchPodCreatingAsNode(ctx, n, item.pod)
				if s.cfg.EmitPodEvents {
					go s.emitPodEvent(ctx, n, item.pod, "Pulled", fmt.Sprintf("Container image already present on machine %s", n.Name))
				}

				// Stage 2: Container started, readiness probe not yet passing (Running, Started=true, Ready=false)
				stage2Delay := max(60*time.Millisecond, delay/3)
				select {
				case <-ctx.Done():
					return
				case <-time.After(stage2Delay):
				}
				if s.isPodTerminating(item.pod.UID) {
					continue
				}
				_ = s.patchPodStartedNotReadyAsNode(ctx, n, item.pod)
				if s.cfg.EmitPodEvents {
					go s.emitPodEvent(ctx, n, item.pod, "Created", fmt.Sprintf("Created container %s on node %s", item.pod.Name, n.Name))
				}

				// Stage 3: Readiness probe passes (Running, Ready=true)
				stage3Delay := max(50*time.Millisecond, delay-stage1Delay-stage2Delay)
				select {
				case <-ctx.Done():
					return
				case <-time.After(stage3Delay):
				}
				if s.isPodTerminating(item.pod.UID) {
					continue
				}
			} else if delay > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(delay):
				}
			}

			if err := s.patchPodRunningAsNode(ctx, n, item.pod); err != nil {
				s.seenPods.Delete(item.pod.UID)
				continue
			}
			if !item.isJobPod {
				s.seenPods.Store(item.pod.UID, podStateRunning)
			}
			if s.cfg.EmitPodEvents {
				go s.emitPodEvent(ctx, n, item.pod, "Started", fmt.Sprintf("Started container %s on node %s", item.pod.Name, n.Name))
			}
			if item.isJobPod {
				if s.cfg.JobCompleteDelay > 0 {
					time.Sleep(s.cfg.JobCompleteDelay)
				}
				if err := s.patchPodSucceededAsNode(ctx, n, item.pod); err != nil {
					s.seenPods.Delete(item.pod.UID)
				} else {
					s.seenPods.Store(item.pod.UID, podStateSucceeded)
				}
			}
		}
	}
}

func (s *Simulator) emitPodEvent(ctx context.Context, n *SimNode, pod *corev1.Pod, reason, msg string) {
	now := metav1.Now()
	evName := fmt.Sprintf("%s.%s.%x", pod.Name, strings.ToLower(reason), now.UnixNano())
	ev := &corev1.Event{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Event",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      evName,
			Namespace: pod.Namespace,
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:            "Pod",
			Namespace:       pod.Namespace,
			Name:            pod.Name,
			UID:             pod.UID,
			APIVersion:      "v1",
			ResourceVersion: pod.ResourceVersion,
			FieldPath:       "spec.containers{0}",
		},
		Reason:  reason,
		Message: msg,
		Source: corev1.EventSource{
			Component: "kubelet",
			Host:      n.Name,
		},
		FirstTimestamp:      now,
		LastTimestamp:       now,
		Count:               1,
		Type:                corev1.EventTypeNormal,
		ReportingController: "kubelet",
		ReportingInstance:   n.Name,
	}
	var buf bytes.Buffer
	if err := protoSerializer.Encode(ev, &buf); err != nil {
		return
	}
	u := fmt.Sprintf("%s/api/v1/namespaces/%s/events?timeout=5s", s.baseURL, pod.Namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", protobufContentType)
	req.Header.Set("Accept", protobufContentType)
	resp, err := s.adminHTTP.Do(req)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		s.stats.EventPOSTs.Add(1)
	}
}

func (s *Simulator) patchPodCreatingAsNode(ctx context.Context, n *SimNode, pod *corev1.Pod) error {
	now := metav1.Now()
	podIP := fmt.Sprintf("10.%d.%d.%d", 64+(n.Index/256), n.Index%256, 2+(int(pod.UID[0])%250))
	started := false
	cStatuses := make([]corev1.ContainerStatus, len(pod.Spec.Containers))
	for i, c := range pod.Spec.Containers {
		cStatuses[i] = corev1.ContainerStatus{
			Name:    c.Name,
			Image:   c.Image,
			Ready:   false,
			Started: &started,
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
			},
		}
	}
	patchBody := map[string]any{
		"status": corev1.PodStatus{
			Phase:     corev1.PodPending,
			HostIP:    n.InternalIP,
			HostIPs:   []corev1.HostIP{{IP: n.InternalIP}},
			PodIP:     podIP,
			PodIPs:    []corev1.PodIP{{IP: podIP}},
			StartTime: &now,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReadyToStartContainers, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "ContainersNotReady", LastTransitionTime: now},
				{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, Reason: "ContainersNotReady", LastTransitionTime: now},
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: now},
			},
			ContainerStatuses: cStatuses,
		},
	}
	raw, _ := json.Marshal(patchBody)
	return s.doNodePodStatusPatchWithRetry(ctx, n, pod.Namespace, pod.Name, raw)
}

func (s *Simulator) patchPodStartedNotReadyAsNode(ctx context.Context, n *SimNode, pod *corev1.Pod) error {
	now := metav1.Now()
	podIP := fmt.Sprintf("10.%d.%d.%d", 64+(n.Index/256), n.Index%256, 2+(int(pod.UID[0])%250))
	started := true
	cStatuses := make([]corev1.ContainerStatus, len(pod.Spec.Containers))
	for i, c := range pod.Spec.Containers {
		cStatuses[i] = corev1.ContainerStatus{
			Name:        c.Name,
			Image:       c.Image,
			ImageID:     "registry.k8s.io/pause@sha256:7031c1b283388d2c2e09b57badb803c05ebed362dc88d84b480cc47f72a21097",
			ContainerID: fmt.Sprintf("containerd://vibe-%s-%d-a1b2c3d4e5f60718293a4b5c6d7e8f90", pod.UID[:8], i),
			Ready:       false,
			Started:     &started,
			State: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{StartedAt: now},
			},
		}
	}
	patchBody := map[string]any{
		"status": corev1.PodStatus{
			Phase:     corev1.PodRunning,
			HostIP:    n.InternalIP,
			HostIPs:   []corev1.HostIP{{IP: n.InternalIP}},
			PodIP:     podIP,
			PodIPs:    []corev1.PodIP{{IP: podIP}},
			StartTime: &now,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReadyToStartContainers, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "ContainersNotReady", LastTransitionTime: now},
				{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, Reason: "ContainersNotReady", LastTransitionTime: now},
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: now},
			},
			ContainerStatuses: cStatuses,
		},
	}
	raw, _ := json.Marshal(patchBody)
	return s.doNodePodStatusPatchWithRetry(ctx, n, pod.Namespace, pod.Name, raw)
}

func (s *Simulator) patchPodTerminatingAsNode(ctx context.Context, n *SimNode, pod *corev1.Pod) error {
	now := metav1.Now()
	started := false
	cStatuses := make([]corev1.ContainerStatus, len(pod.Spec.Containers))
	for i, c := range pod.Spec.Containers {
		cStatuses[i] = corev1.ContainerStatus{
			Name:        c.Name,
			Image:       c.Image,
			ImageID:     "registry.k8s.io/pause@sha256:7031c1b283388d2c2e09b57badb803c05ebed362dc88d84b480cc47f72a21097",
			ContainerID: fmt.Sprintf("containerd://vibe-%s-%d-a1b2c3d4e5f60718293a4b5c6d7e8f90", pod.UID[:8], i),
			Ready:       false,
			Started:     &started,
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   0,
					Reason:     "Completed",
					StartedAt:  now,
					FinishedAt: now,
				},
			},
		}
	}
	patchBody := map[string]any{
		"status": corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReadyToStartContainers, Status: corev1.ConditionFalse, LastTransitionTime: now},
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "ContainersNotReady", LastTransitionTime: now},
				{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, Reason: "ContainersNotReady", LastTransitionTime: now},
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: now},
			},
			ContainerStatuses: cStatuses,
		},
	}
	raw, _ := json.Marshal(patchBody)
	return s.doNodePodStatusPatchWithRetry(ctx, n, pod.Namespace, pod.Name, raw)
}

func (s *Simulator) patchPodRunningAsNode(ctx context.Context, n *SimNode, pod *corev1.Pod) error {
	now := metav1.Now()
	podIP := fmt.Sprintf("10.%d.%d.%d", 64+(n.Index/256), n.Index%256, 2+(int(pod.UID[0])%250))

	cStatuses := make([]corev1.ContainerStatus, len(pod.Spec.Containers))
	started := true
	for i, c := range pod.Spec.Containers {
		cStatuses[i] = corev1.ContainerStatus{
			Name:        c.Name,
			Image:       c.Image,
			ImageID:     "registry.k8s.io/pause@sha256:7031c1b283388d2c2e09b57badb803c05ebed362dc88d84b480cc47f72a21097",
			ContainerID: fmt.Sprintf("containerd://vibe-%s-%d-a1b2c3d4e5f60718293a4b5c6d7e8f90", pod.UID[:8], i),
			Ready:       true,
			Started:     &started,
			State: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{StartedAt: now},
			},
		}
	}

	initStatuses := make([]corev1.ContainerStatus, len(pod.Spec.InitContainers))
	for i, c := range pod.Spec.InitContainers {
		initStatuses[i] = corev1.ContainerStatus{
			Name:        c.Name,
			Image:       c.Image,
			ImageID:     "registry.k8s.io/pause@sha256:7031c1b283388d2c2e09b57badb803c05ebed362dc88d84b480cc47f72a21097",
			ContainerID: fmt.Sprintf("containerd://vibe-init-%s-%d-a1b2c3d4e5f60718", pod.UID[:8], i),
			Ready:       true,
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   0,
					Reason:     "Completed",
					StartedAt:  now,
					FinishedAt: now,
				},
			},
		}
	}

	patchBody := map[string]any{
		"status": corev1.PodStatus{
			Phase:     corev1.PodRunning,
			HostIP:    n.InternalIP,
			HostIPs:   []corev1.HostIP{{IP: n.InternalIP}},
			PodIP:     podIP,
			PodIPs:    []corev1.PodIP{{IP: podIP}},
			StartTime: &now,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReadyToStartContainers, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: now},
			},
			ContainerStatuses:     cStatuses,
			InitContainerStatuses: initStatuses,
		},
	}
	raw, _ := json.Marshal(patchBody)
	return s.doNodePodStatusPatchWithRetry(ctx, n, pod.Namespace, pod.Name, raw)
}

func (s *Simulator) patchPodSucceededAsNode(ctx context.Context, n *SimNode, pod *corev1.Pod) error {
	now := metav1.Now()
	cStatuses := make([]corev1.ContainerStatus, len(pod.Spec.Containers))
	started := false
	for i, c := range pod.Spec.Containers {
		cStatuses[i] = corev1.ContainerStatus{
			Name:        c.Name,
			Image:       c.Image,
			ImageID:     "docker-pullable://" + c.Image,
			ContainerID: fmt.Sprintf("containerd://vibe-%s-%d", pod.UID[:8], i),
			Ready:       false,
			Started:     &started,
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   0,
					Reason:     "Completed",
					StartedAt:  now,
					FinishedAt: now,
				},
			},
		}
	}
	patchBody := map[string]any{
		"status": corev1.PodStatus{
			Phase: corev1.PodSucceeded,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "PodCompleted", LastTransitionTime: now},
				{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, Reason: "PodCompleted", LastTransitionTime: now},
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: now},
			},
			ContainerStatuses: cStatuses,
		},
	}
	raw, _ := json.Marshal(patchBody)
	return s.doNodePodStatusPatchWithRetry(ctx, n, pod.Namespace, pod.Name, raw)
}

func (s *Simulator) doNodePodStatusPatchWithRetry(ctx context.Context, n *SimNode, ns, name string, payload []byte) error {
	u := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s/status?timeout=10s", s.baseURL, ns, name)
	backoff := 40 * time.Millisecond
	for attempt := 0; attempt < 10; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.podMutateSem <- struct{}{}:
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPatch, u, bytes.NewReader(payload))
		if err != nil {
			<-s.podMutateSem
			return err
		}
		req.Header.Set("Content-Type", smpContentType)
		req.Header.Set("Accept", protobufContentType)

		resp, err := n.KubeletClient.Do(req)
		<-s.podMutateSem
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			s.stats.PodPATCHes.Add(1)
			return nil
		}
		if resp.StatusCode == http.StatusNotFound {
			return nil
		}
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			s.stats.PodAuthzRetries.Add(1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff = min(backoff*2, 400*time.Millisecond)
				continue
			}
		}
		return fmt.Errorf("pod status PATCH %s/%s returned %d", ns, name, resp.StatusCode)
	}
	return fmt.Errorf("pod status PATCH %s/%s exhausted retries", ns, name)
}

func (s *Simulator) deleteTerminatingPodAsNode(ctx context.Context, n *SimNode, pod *corev1.Pod) error {
	zero := int64(0)
	delOpts := &metav1.DeleteOptions{
		TypeMeta:           metav1.TypeMeta{APIVersion: "v1", Kind: "DeleteOptions"},
		GracePeriodSeconds: &zero,
		Preconditions:      &metav1.Preconditions{UID: &pod.UID},
	}
	var buf bytes.Buffer
	if err := protoSerializer.Encode(delOpts, &buf); err != nil {
		return err
	}
	u := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", s.baseURL, pod.Namespace, pod.Name)
	backoff := 40 * time.Millisecond
	for attempt := 0; attempt < 8; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.podMutateSem <- struct{}{}:
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, bytes.NewReader(buf.Bytes()))
		if err != nil {
			<-s.podMutateSem
			return err
		}
		req.Header.Set("Content-Type", protobufContentType)
		req.Header.Set("Accept", protobufContentType)
		resp, err := n.KubeletClient.Do(req)
		<-s.podMutateSem
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if (resp.StatusCode >= 200 && resp.StatusCode < 300) || resp.StatusCode == http.StatusNotFound {
			s.stats.PodDELETEs.Add(1)
			return nil
		}
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			s.stats.PodAuthzRetries.Add(1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff = min(backoff*2, 400*time.Millisecond)
				continue
			}
		}
		return fmt.Errorf("pod DELETE %s/%s returned %d", pod.Namespace, pod.Name, resp.StatusCode)
	}
	return fmt.Errorf("pod DELETE %s/%s exhausted retries", pod.Namespace, pod.Name)
}

func (s *Simulator) watchAndBindPVCs(ctx context.Context) {
	for ctx.Err() == nil {
		w, err := s.adminClient.CoreV1().PersistentVolumeClaims("").Watch(ctx, metav1.ListOptions{ResourceVersion: "0"})
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		for ev := range w.ResultChan() {
			pvc, ok := ev.Object.(*corev1.PersistentVolumeClaim)
			if !ok || pvc.Status.Phase != corev1.ClaimPending || pvc.DeletionTimestamp != nil {
				continue
			}
			go s.bindSinglePVC(ctx, pvc)
		}
	}
}

func (s *Simulator) bindSinglePVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) {
	pvName := fmt.Sprintf("vibe-pv-%s-%s", pvc.Namespace, pvc.Name)
	storageQty := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if storageQty.IsZero() {
		storageQty = resource.MustParse("1Gi")
	}
	scName := ""
	if pvc.Spec.StorageClassName != nil {
		scName = *pvc.Spec.StorageClassName
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: storageQty},
			AccessModes:                   pvc.Spec.AccessModes,
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              scName,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/tmp/" + pvName},
			},
			ClaimRef: &corev1.ObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Namespace:  pvc.Namespace,
				Name:       pvc.Name,
				UID:        pvc.UID,
			},
		},
	}
	_, err := s.adminClient.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
	if err == nil || apierrors.IsAlreadyExists(err) {
		s.stats.PVCsBound.Add(1)
	}
}

func (s *Simulator) reportTelemetryLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var prevLeasePUTs, prevNodePATCHes, prevPodPATCHes, prevPodDELETEs, prevEventPOSTs, prevWatchBytes uint64
	lastTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			dt := now.Sub(lastTime).Seconds()
			lastTime = now

			curLease := s.stats.LeasePUTs.Load()
			curNodePatch := s.stats.NodePATCHes.Load()
			curPodPatch := s.stats.PodPATCHes.Load()
			curPodDel := s.stats.PodDELETEs.Load()
			curEventPost := s.stats.EventPOSTs.Load()
			curWatchBytes := s.stats.WatchBytes.Load()

			leaseRate := float64(curLease-prevLeasePUTs) / dt
			nodePatchRate := float64(curNodePatch-prevNodePATCHes) / dt
			podPatchRate := float64(curPodPatch-prevPodPATCHes) / dt
			podDelRate := float64(curPodDel-prevPodDELETEs) / dt
			eventRate := float64(curEventPost-prevEventPOSTs) / dt
			watchMBps := float64(curWatchBytes-prevWatchBytes) / dt / (1024 * 1024)

			prevLeasePUTs = curLease
			prevNodePATCHes = curNodePatch
			prevPodPATCHes = curPodPatch
			prevPodDELETEs = curPodDel
			prevEventPOSTs = curEventPost
			prevWatchBytes = curWatchBytes

			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			klog.Infof("[vibe-kubemark] nodes=%d activeWatches=%d watchDrain=%.2f MiB/s leasePUT=%.1f/s (err=%d) nodePATCH=%.1f/s (total=%d) podPATCH=%.1f/s (total=%d, retry=%d) podDEL=%.1f/s (total=%d) events=%.1f/s (total=%d) pvcBound=%d workQ=%d heap=%dMiB goroutines=%d",
				s.cfg.NumNodes,
				s.stats.ActiveWatches.Load(),
				watchMBps,
				leaseRate,
				s.stats.LeaseErrors.Load(),
				nodePatchRate,
				curNodePatch,
				podPatchRate,
				curPodPatch,
				s.stats.PodAuthzRetries.Load(),
				podDelRate,
				curPodDel,
				eventRate,
				curEventPost,
				s.stats.PVCsBound.Load(),
				len(s.podWorkCh),
				m.HeapAlloc/(1024*1024),
				runtime.NumGoroutine(),
			)
		}
	}
}

func (s *Simulator) cleanupNodes() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	klog.Infof("Cleaning up %d simulated nodes...", s.cfg.NumNodes)
	_ = wait.PollUntilContextTimeout(ctx, time.Second, 25*time.Second, true, func(ctx context.Context) (bool, error) {
		err := s.adminClient.CoreV1().Nodes().DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
			LabelSelector: "vibe.k8s.io/simulated=true",
		})
		return err == nil, nil
	})
}
