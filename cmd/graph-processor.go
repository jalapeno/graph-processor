package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"

	"github.com/cisco-open/jalapeno/topology/kafkanotifier"
	"github.com/golang/glog"
	"github.com/jalapeno/graph-processor/pkg/arangodb"
	"github.com/jalapeno/graph-processor/pkg/kafkamessenger"

	_ "net/http/pprof"
)

const (
	// userFile defines the name of file containing base64 encoded user name
	userFile = "./credentials/.username"
	// passFile defines the name of file containing base64 encoded password
	passFile = "./credentials/.password"
	// MAXUSERNAME defines maximum length of ArangoDB user name
	MAXUSERNAME = 256
	// MAXPASS defines maximum length of ArangoDB password
	MAXPASS = 256
)

var (
	msgSrvAddr string
	dbSrvAddr  string
	dbName     string
	dbUser     string
	dbPass     string

	// IGP collections
	lsprefix   string
	lslink     string
	lssrv6sid  string
	lsnode     string
	igpDomain  string
	igpNode    string
	igpv4Graph string
	igpv6Graph string

	// BGP collections
	peer    string
	bgpNode string

	// BGP IPv4 collections
	ebgpprefixV4 string
	inetprefixV4 string
	ibgpprefixV4 string
	ipv4Graph    string

	// BGP IPv6 collections
	ebgpprefixV6 string
	inetprefixV6 string
	ibgpprefixV6 string
	ipv6Graph    string
)

func init() {
	runtime.GOMAXPROCS(1)
	flag.StringVar(&msgSrvAddr, "message-server", "", "URL to the messages supplying server")
	flag.StringVar(&dbSrvAddr, "database-server", "", "{dns name}:port or X.X.X.X:port of the graph database")
	flag.StringVar(&dbName, "database-name", "", "DB name")
	flag.StringVar(&dbUser, "database-user", "", "DB User name")
	flag.StringVar(&dbPass, "database-pass", "", "DB User's password")

	// for local testing
	// flag.StringVar(&msgSrvAddr, "message-server", "198.18.133.102:30092", "URL to the messages supplying server")
	// flag.StringVar(&dbSrvAddr, "database-server", "http://198.18.133.102:30852", "{dns name}:port or X.X.X.X:port of the graph database")
	// flag.StringVar(&dbName, "database-name", "jalapeno", "DB name")
	// flag.StringVar(&dbUser, "database-user", "root", "DB User name")
	// flag.StringVar(&dbPass, "database-pass", "jalapeno", "DB User's password")

	// IGP collections
	flag.StringVar(&lsprefix, "ls_prefix", "ls_prefix", "ls_prefix Collection name, default: \"ls_prefix\"")
	flag.StringVar(&lslink, "ls_link", "ls_link", "ls_link Collection name, default \"ls_link\"")
	flag.StringVar(&lssrv6sid, "ls_srv6_sid", "ls_srv6_sid", "ls_srv6_sid Collection name, default: \"ls_srv6_sid\"")
	flag.StringVar(&lsnode, "ls_node", "ls_node", "ls_node Collection name, default \"ls_node\"")
	flag.StringVar(&igpDomain, "igp_domain", "igp_domain", "igp_domain Collection name, default \"igp_domain\"")
	flag.StringVar(&igpNode, "igp_node", "igp_node", "igp_node Collection name, default \"igp_node\"")
	flag.StringVar(&igpv4Graph, "igpv4_graph", "igpv4_graph", "igpv4_graph Collection name, default \"igpv4_graph\"")
	flag.StringVar(&igpv6Graph, "igpv6_graph", "igpv6_graph", "igpv6_graph Collection name, default \"igpv6_graph\"")

	// BGP collections
	flag.StringVar(&peer, "peer-name", "peer", "peer Collection name, default \"peer\"")
	flag.StringVar(&bgpNode, "bgp-node-name", "bgp_node", "bgp node Collection name, default \"bgp_node\"")

	// BGP IPv4 collections
	flag.StringVar(&ebgpprefixV4, "ebgpprefixv4-prefix-name", "ebgp_prefix_v4", "ebgpprefix v4 Collection name, default \"ebgp_prefix_v4\"")
	flag.StringVar(&inetprefixV4, "inetprefixv4-prefix-name", "inet_prefix_v4", "inet prefix v4 Collection name, default \"inet_prefix_v4\"")
	flag.StringVar(&ibgpprefixV4, "ibgpprefixv4-prefix-name", "ibgp_prefix_v4", "ibgp prefix v4 Collection name, default \"ibgp_prefix_v4\"")
	flag.StringVar(&ipv4Graph, "ipv4-graph", "ipv4_graph", "ipv4_graph Collection name, default \"ipv4_graph\"")

	// BGP IPv6 collections
	flag.StringVar(&ebgpprefixV6, "ebgpprefixv6-prefix-name", "ebgp_prefix_v6", "ebgpprefix v6 Collection name, default \"ebgp_prefix_v6\"")
	flag.StringVar(&inetprefixV6, "inetprefixv6-prefix-name", "inet_prefix_v6", "inet prefix v6 Collection name, default \"inet_prefix_v6\"")
	flag.StringVar(&ibgpprefixV6, "ibgpprefixv6-prefix-name", "ibgp_prefix_v6", "ibgpprefix v6 Collection name, default \"ibgp_prefix_v6\"")
	flag.StringVar(&ipv6Graph, "ipv6-graph", "ipv6_graph", "ipv6_graph Collection name, default \"ipv6_graph\"")
}

var (
	onlyOneSignalHandler = make(chan struct{})
	shutdownSignals      = []os.Signal{os.Interrupt}
)

func setupSignalHandler() (stopCh <-chan struct{}) {
	close(onlyOneSignalHandler) // panics when called twice

	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()

	return stop
}

func main() {
	flag.Parse()
	_ = flag.Set("logtostderr", "true")

	// validateDBCreds check if the user name and the password are provided either as
	// command line parameters or via files. If both are provided command line parameters
	// will be used, if neither, processor will fail.
	if err := validateDBCreds(); err != nil {
		glog.Errorf("failed to validate the database credentials with error: %+v", err)
		os.Exit(1)
	}

	// initialize kafkanotifier to write back processed events into ls_node_edge_events topic
	notifier, err := kafkanotifier.NewKafkaNotifier(msgSrvAddr)
	if err != nil {
		glog.Errorf("failed to initialize events notifier with error: %+v", err)
		os.Exit(1)
	}

	dbSrv, err := arangodb.NewDBSrvClient(dbSrvAddr, dbUser, dbPass, dbName, lsprefix, lslink,
		lssrv6sid, lsnode, igpDomain, igpNode, igpv4Graph, igpv6Graph, peer, bgpNode,
		ebgpprefixV4, inetprefixV4, ibgpprefixV4, ipv4Graph,
		ebgpprefixV6, inetprefixV6, ibgpprefixV6, ipv6Graph, notifier)
	if err != nil {
		glog.Errorf("failed to initialize database client with error: %+v", err)
		os.Exit(1)
	}

	if err := dbSrv.Start(); err != nil {
		if err != nil {
			glog.Errorf("failed to connect to database with error: %+v", err)
			os.Exit(1)
		}
	}

	// Initializing messenger process
	msgSrv, err := kafkamessenger.NewKafkaMessenger(msgSrvAddr, dbSrv.GetInterface())
	if err != nil {
		glog.Errorf("failed to initialize message server with error: %+v", err)
		os.Exit(1)
	}

	msgSrv.Start()

	stopCh := setupSignalHandler()
	<-stopCh

	msgSrv.Stop()
	dbSrv.Stop()

	os.Exit(0)
}

func validateDBCreds() error {
	// Attempting to access username and password files.
	u, err := readAndDecode(userFile, MAXUSERNAME)
	if err != nil {
		if dbUser != "" && dbPass != "" {
			return nil
		}
		return fmt.Errorf("failed to access %s with error: %+v and no username and password provided via command line arguments", userFile, err)
	}
	p, err := readAndDecode(passFile, MAXPASS)
	if err != nil {
		if dbUser != "" && dbPass != "" {
			return nil
		}
		return fmt.Errorf("failed to access %s with error: %+v and no username and password provided via command line arguments", passFile, err)
	}
	dbUser, dbPass = u, p

	return nil
}

func readAndDecode(fn string, max int) (string, error) {
	f, err := os.Open(fn)
	if err != nil {
		return "", err
	}
	defer f.Close()
	l, err := f.Stat()
	if err != nil {
		return "", err
	}
	b := make([]byte, int(l.Size()))
	n, err := io.ReadFull(f, b)
	if err != nil {
		return "", err
	}
	if n > max {
		return "", fmt.Errorf("length of data %d exceeds maximum acceptable length: %d", n, max)
	}
	b = b[:n]

	return string(b), nil
}
