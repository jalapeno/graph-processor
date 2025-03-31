package arangodb

import (
	"context"
	"encoding/json"

	driver "github.com/arangodb/go-driver"
	"github.com/cisco-open/jalapeno/topology/dbclient"
	"github.com/cisco-open/jalapeno/topology/kafkanotifier"
	notifier "github.com/cisco-open/jalapeno/topology/kafkanotifier"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/tools"
)

type arangoDB struct {
	dbclient.DB
	*ArangoConn
	stop       chan struct{}
	lsprefix   driver.Collection
	lslink     driver.Collection
	lssrv6sid  driver.Collection
	lsnode     driver.Collection
	igpDomain  driver.Collection
	igpNode    driver.Collection
	igpv4Edge  driver.Collection
	igpv6Edge  driver.Collection
	igpv4Graph driver.Graph
	igpv6Graph driver.Graph

	peer    driver.Collection
	bgpNode driver.Collection

	ebgpprefixV4 driver.Collection
	inetprefixV4 driver.Collection
	ibgpprefixV4 driver.Collection
	ipv4Edge     driver.Collection
	ipv4Graph    driver.Graph

	ebgpprefixV6 driver.Collection
	inetprefixV6 driver.Collection
	ibgpprefixV6 driver.Collection
	ipv6Edge     driver.Collection
	ipv6Graph    driver.Graph

	notifier kafkanotifier.Event
}

// NewDBSrvClient returns an instance of a DB server client process
func NewDBSrvClient(arangoSrv, user, pass, dbname, lsprefix, lslink, lssrv6sid, lsnode,
	igpDomain string, igpNode string, igpv4Graph string, igpv6Graph string,
	peer string, bgpNode string, ebgpprefixV4 string, inetprefixV4 string, ibgpprefixV4 string, ipv4Graph string,
	ebgpprefixV6 string, inetprefixV6 string, ibgpprefixV6 string, ipv6Graph string,
	notifier kafkanotifier.Event) (dbclient.Srv, error) {
	if err := tools.URLAddrValidation(arangoSrv); err != nil {
		return nil, err
	}
	arangoConn, err := NewArango(ArangoConfig{
		URL:      arangoSrv,
		User:     user,
		Password: pass,
		Database: dbname,
	})
	if err != nil {
		return nil, err
	}
	arango := &arangoDB{
		stop: make(chan struct{}),
	}
	arango.DB = arango
	arango.ArangoConn = arangoConn

	// Check if base collections exist, if not fail as Jalapeno topology is not running
	arango.lsprefix, err = arango.db.Collection(context.TODO(), lsprefix)
	if err != nil {
		return nil, err
	}
	arango.lslink, err = arango.db.Collection(context.TODO(), lslink)
	if err != nil {
		return nil, err
	}
	arango.lssrv6sid, err = arango.db.Collection(context.TODO(), lssrv6sid)
	if err != nil {
		return nil, err
	}
	arango.lsnode, err = arango.db.Collection(context.TODO(), lsnode)
	if err != nil {
		return nil, err
	}

	arango.peer, err = arango.db.Collection(context.TODO(), peer)
	if err != nil {
		return nil, err
	}

	// check for igp_node collection
	found, err := arango.db.CollectionExists(context.TODO(), igpNode)
	if err != nil {
		return nil, err
	}
	if found {
		// Collection exists, get a reference to it
		arango.igpNode, err = arango.db.Collection(context.TODO(), igpNode)
		if err != nil {
			return nil, err
		}
	} else {
		// Collection doesn't exist, create it
		var igpNode_options = &driver.CreateCollectionOptions{ /* ... */ }
		glog.V(5).Infof("igp_node not found, creating")
		arango.igpNode, err = arango.db.CreateCollection(context.TODO(), "igp_node", igpNode_options)
		if err != nil {
			return nil, err
		}
	}

	// check for igp_domain collection
	found, err = arango.db.CollectionExists(context.TODO(), igpDomain)
	if err != nil {
		return nil, err
	}
	if found {
		// Collection exists, get a reference to it
		arango.igpDomain, err = arango.db.Collection(context.TODO(), igpDomain)
		if err != nil {
			return nil, err
		}
	} else {
		// Collection doesn't exist, create it
		var igpDomain_options = &driver.CreateCollectionOptions{ /* ... */ }
		glog.V(5).Infof("igp_domain not found, creating")
		arango.igpDomain, err = arango.db.CreateCollection(context.TODO(), "igp_domain", igpDomain_options)
		if err != nil {
			return nil, err
		}
	}

	// check for bgp_node collection
	found, err = arango.db.CollectionExists(context.TODO(), bgpNode)
	if err != nil {
		return nil, err
	}
	if found {
		// Collection exists, get a reference to it
		arango.bgpNode, err = arango.db.Collection(context.TODO(), bgpNode)
		if err != nil {
			return nil, err
		}
	} else {
		// Collection doesn't exist, create it
		var bgpNode_options = &driver.CreateCollectionOptions{ /* ... */ }
		glog.V(5).Infof("bgp_node not found, creating")
		arango.bgpNode, err = arango.db.CreateCollection(context.TODO(), "bgp_node", bgpNode_options)
		if err != nil {
			return nil, err
		}
	}

	// check for ebgp_prefix_v4 collection
	found, err = arango.db.CollectionExists(context.TODO(), ebgpprefixV4)
	if err != nil {
		return nil, err
	}
	if found {
		// Collection exists, get a reference to it
		arango.ebgpprefixV4, err = arango.db.Collection(context.TODO(), ebgpprefixV4)
		if err != nil {
			return nil, err
		}
	} else {
		// Collection doesn't exist, create it
		var ebgpprefixV4_options = &driver.CreateCollectionOptions{ /* ... */ }
		glog.V(5).Infof("ebgp_prefix_v4 not found, creating")
		arango.ebgpprefixV4, err = arango.db.CreateCollection(context.TODO(), ebgpprefixV4, ebgpprefixV4_options)
		if err != nil {
			return nil, err
		}
	}

	// check for inet_prefix_v4 collection
	found, err = arango.db.CollectionExists(context.TODO(), inetprefixV4)
	if err != nil {
		return nil, err
	}
	if found {
		// Collection exists, get a reference to it
		arango.inetprefixV4, err = arango.db.Collection(context.TODO(), inetprefixV4)
		if err != nil {
			return nil, err
		}
	} else {
		// Collection doesn't exist, create it
		var inetprefixV4_options = &driver.CreateCollectionOptions{ /* ... */ }
		glog.V(5).Infof("inet_prefix_v4 not found, creating")
		arango.inetprefixV4, err = arango.db.CreateCollection(context.TODO(), inetprefixV4, inetprefixV4_options)
		if err != nil {
			return nil, err
		}
	}

	// check for ibgp_prefix_v4 collection
	found, err = arango.db.CollectionExists(context.TODO(), ibgpprefixV4)
	if err != nil {
		return nil, err
	}
	if found {
		// Collection exists, get a reference to it
		arango.ibgpprefixV4, err = arango.db.Collection(context.TODO(), ibgpprefixV4)
		if err != nil {
			return nil, err
		}
	} else {
		// Collection doesn't exist, create it
		var ibgpprefixV4_options = &driver.CreateCollectionOptions{ /* ... */ }
		glog.V(5).Infof("ibgp_prefix_v4 not found, creating")
		arango.ibgpprefixV4, err = arango.db.CreateCollection(context.TODO(), ibgpprefixV4, ibgpprefixV4_options)
		if err != nil {
			return nil, err
		}
	}

	// check for ebgp_prefix_v6 collection
	found, err = arango.db.CollectionExists(context.TODO(), ebgpprefixV6)
	if err != nil {
		return nil, err
	}
	if found {
		// Collection exists, get a reference to it
		arango.ebgpprefixV6, err = arango.db.Collection(context.TODO(), ebgpprefixV6)
		if err != nil {
			return nil, err
		}
	} else {
		// Collection doesn't exist, create it
		var ebgpprefixV6_options = &driver.CreateCollectionOptions{ /* ... */ }
		glog.V(5).Infof("ebgp_prefix_v6 not found, creating")
		arango.ebgpprefixV6, err = arango.db.CreateCollection(context.TODO(), ebgpprefixV6, ebgpprefixV6_options)
		if err != nil {
			return nil, err
		}
	}

	// check for inet_prefix_v6 collection
	found, err = arango.db.CollectionExists(context.TODO(), inetprefixV6)
	if err != nil {
		return nil, err
	}
	if found {
		// Collection exists, get a reference to it
		arango.inetprefixV6, err = arango.db.Collection(context.TODO(), inetprefixV6)
		if err != nil {
			return nil, err
		}
	} else {
		// Collection doesn't exist, create it
		var inetprefixV6_options = &driver.CreateCollectionOptions{ /* ... */ }
		glog.V(5).Infof("inet_prefix_v6 not found, creating")
		arango.inetprefixV6, err = arango.db.CreateCollection(context.TODO(), inetprefixV6, inetprefixV6_options)
		if err != nil {
			return nil, err
		}
	}

	// check for ibgp_prefix_v6 collection
	found, err = arango.db.CollectionExists(context.TODO(), ibgpprefixV6)
	if err != nil {
		return nil, err
	}
	if found {
		// Collection exists, get a reference to it
		arango.ibgpprefixV6, err = arango.db.Collection(context.TODO(), ibgpprefixV6)
		if err != nil {
			return nil, err
		}
	} else {
		// Collection doesn't exist, create it
		var ibgpprefixV6_options = &driver.CreateCollectionOptions{ /* ... */ }
		glog.V(5).Infof("ibgp_prefix_v6 not found, creating")
		arango.ibgpprefixV6, err = arango.db.CreateCollection(context.TODO(), ibgpprefixV6, ibgpprefixV6_options)
		if err != nil {
			return nil, err
		}
	}

	// Handle IGPv4 graph and edge collection
	found, err = arango.db.GraphExists(context.TODO(), igpv4Graph)
	if err != nil {
		return nil, err
	}
	if found {
		// Get reference to existing graph
		arango.igpv4Graph, err = arango.db.Graph(context.TODO(), igpv4Graph)
		if err != nil {
			return nil, err
		}
		// Get reference to existing edge collection
		arango.igpv4Edge, err = arango.db.Collection(context.TODO(), "igpv4_graph")
		if err != nil {
			return nil, err
		}
		glog.V(5).Infof("Found existing graph and edge collection: %s", igpv4Graph)
	} else {
		// Create edge collection first
		var edgeOptions = &driver.CreateCollectionOptions{Type: driver.CollectionTypeEdge}
		arango.igpv4Edge, err = arango.db.CreateCollection(context.TODO(), "igpv4_graph", edgeOptions)
		if err != nil {
			return nil, err
		}

		// Create graph with edge definition
		var edgeDefinition driver.EdgeDefinition
		edgeDefinition.Collection = "igpv4_graph"
		edgeDefinition.From = []string{"igp_node"}
		edgeDefinition.To = []string{"igp_node"}

		var options driver.CreateGraphOptions
		options.OrphanVertexCollections = []string{"ls_srv6_sid", "ls_prefix"}
		options.EdgeDefinitions = []driver.EdgeDefinition{edgeDefinition}

		arango.igpv4Graph, err = arango.db.CreateGraph(context.TODO(), igpv4Graph, &options)
		if err != nil {
			return nil, err
		}
		glog.V(5).Infof("Created new graph and edge collection: %s", igpv4Graph)
	}

	// Handle IGPv6 graph and edge collection
	found, err = arango.db.GraphExists(context.TODO(), igpv6Graph)
	if err != nil {
		return nil, err
	}
	if found {
		// Get reference to existing graph
		arango.igpv6Graph, err = arango.db.Graph(context.TODO(), igpv6Graph)
		if err != nil {
			return nil, err
		}
		// Get reference to existing edge collection
		arango.igpv6Edge, err = arango.db.Collection(context.TODO(), "igpv6_graph")
		if err != nil {
			return nil, err
		}
		glog.V(5).Infof("Found existing graph and edge collection: %s", igpv6Graph)
	} else {
		// Create edge collection first
		var edgeOptions = &driver.CreateCollectionOptions{Type: driver.CollectionTypeEdge}
		arango.igpv6Edge, err = arango.db.CreateCollection(context.TODO(), "igpv6_graph", edgeOptions)
		if err != nil {
			return nil, err
		}

		// Create graph with edge definition
		var edgeDefinition driver.EdgeDefinition
		edgeDefinition.Collection = "igpv6_graph"
		edgeDefinition.From = []string{"igp_node"}
		edgeDefinition.To = []string{"igp_node"}

		var options driver.CreateGraphOptions
		options.OrphanVertexCollections = []string{"ls_srv6_sid", "ls_prefix"}
		options.EdgeDefinitions = []driver.EdgeDefinition{edgeDefinition}

		arango.igpv6Graph, err = arango.db.CreateGraph(context.TODO(), igpv6Graph, &options)
		if err != nil {
			return nil, err
		}
		glog.V(5).Infof("Created new graph and edge collection: %s", igpv6Graph)
	}

	// Handle IPv4 graph and edge collection
	found, err = arango.db.GraphExists(context.TODO(), ipv4Graph)
	if err != nil {
		return nil, err
	}
	if found {
		// Get reference to existing graph
		arango.ipv4Graph, err = arango.db.Graph(context.TODO(), ipv4Graph)
		if err != nil {
			return nil, err
		}
		// Get reference to existing edge collection
		arango.ipv4Edge, err = arango.db.Collection(context.TODO(), "ipv4_graph")
		if err != nil {
			return nil, err
		}
		glog.V(5).Infof("Found existing graph and edge collection: %s", ipv4Graph)
	} else {
		// Create edge collection first
		var edgeOptions = &driver.CreateCollectionOptions{Type: driver.CollectionTypeEdge}
		arango.ipv4Edge, err = arango.db.CreateCollection(context.TODO(), "ipv4_graph", edgeOptions)
		if err != nil {
			return nil, err
		}

		// Create graph with edge definition
		var edgeDefinition driver.EdgeDefinition
		edgeDefinition.Collection = "ipv4_graph"
		edgeDefinition.From = []string{"bgp_node", "ebgp_prefix_v4", "inet_prefix_v4", "ibgp_prefix_v4"}
		edgeDefinition.To = []string{"bgp_node", "ebgp_prefix_v4", "inet_prefix_v4", "ibgp_prefix_v4"}

		var options driver.CreateGraphOptions
		options.OrphanVertexCollections = []string{"unicast_prefix_v4", "ebgp_prefix_v4", "inet_prefix_v4", "ibgp_prefix_v4"}
		options.EdgeDefinitions = []driver.EdgeDefinition{edgeDefinition}

		arango.ipv4Graph, err = arango.db.CreateGraph(context.TODO(), ipv4Graph, &options)
		if err != nil {
			return nil, err
		}
		glog.V(5).Infof("Created new graph and edge collection: %s", ipv4Graph)
	}

	// Handle IPv6 graph and edge collection
	found, err = arango.db.GraphExists(context.TODO(), ipv6Graph)
	if err != nil {
		return nil, err
	}
	if found {
		// Get reference to existing graph
		arango.ipv6Graph, err = arango.db.Graph(context.TODO(), ipv6Graph)
		if err != nil {
			return nil, err
		}
		// Get reference to existing edge collection
		arango.ipv6Edge, err = arango.db.Collection(context.TODO(), "ipv6_graph")
		if err != nil {
			return nil, err
		}
		glog.V(5).Infof("Found existing graph and edge collection: %s", ipv6Graph)
	} else {
		// Create edge collection first
		var edgeOptions = &driver.CreateCollectionOptions{Type: driver.CollectionTypeEdge}
		arango.ipv6Edge, err = arango.db.CreateCollection(context.TODO(), "ipv6_graph", edgeOptions)
		if err != nil {
			return nil, err
		}

		// Create graph with edge definition
		var edgeDefinition driver.EdgeDefinition
		edgeDefinition.Collection = "ipv6_graph"
		edgeDefinition.From = []string{"bgp_node", "ebgp_prefix_v6", "inet_prefix_v6", "ibgp_prefix_v6"}
		edgeDefinition.To = []string{"bgp_node", "ebgp_prefix_v6", "inet_prefix_v6", "ibgp_prefix_v6"}

		var options driver.CreateGraphOptions
		options.OrphanVertexCollections = []string{"unicast_prefix_v6", "ebgp_prefix_v6", "inet_prefix_v6", "ibgp_prefix_v6"}
		options.EdgeDefinitions = []driver.EdgeDefinition{edgeDefinition}

		arango.ipv6Graph, err = arango.db.CreateGraph(context.TODO(), ipv6Graph, &options)
		if err != nil {
			return nil, err
		}
		glog.V(5).Infof("Created new graph and edge collection: %s", ipv6Graph)
	}

	return arango, nil
}

func (a *arangoDB) Start() error {
	if err := a.loadCollections(); err != nil {
		return err
	}
	glog.Infof("Connected to arango database, starting monitor")
	go a.monitor()

	return nil
}

func (a *arangoDB) Stop() error {
	close(a.stop)

	return nil
}

func (a *arangoDB) GetInterface() dbclient.DB {
	return a.DB
}

func (a *arangoDB) GetArangoDBInterface() *ArangoConn {
	return a.ArangoConn
}

func (a *arangoDB) StoreMessage(msgType dbclient.CollectionType, msg []byte) error {
	event := &notifier.EventMessage{}
	if err := json.Unmarshal(msg, event); err != nil {
		return err
	}
	event.TopicType = msgType
	switch msgType {
	case bmp.LSSRv6SIDMsg:
		return a.lsSRv6SIDHandler(event)
	case bmp.LSNodeMsg:
		return a.lsNodeHandler(event)
	case bmp.LSPrefixMsg:
		return a.lsPrefixHandler(event)
	case bmp.LSLinkMsg:
		return a.lsLinkHandler(event)
	case bmp.PeerStateChangeMsg:
		return a.peerHandler(event)
	case bmp.UnicastPrefixV4Msg:
		return a.unicastPrefixV4Handler(event)
	case bmp.UnicastPrefixV6Msg:
		return a.unicastPrefixV6Handler(event)

	}
	return nil
}

func (a *arangoDB) monitor() {
	for {
		select {
		case <-a.stop:
			// TODO Add clean up of connection with Arango DB
			return
		}
	}
}
