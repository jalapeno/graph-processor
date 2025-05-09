package arangodb

import (
	"context"

	"github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) processEgressV6Peer(ctx context.Context, key string, p *message.PeerStateChange) error {

	glog.Infof("process ebgp session: %s", p.Key)
	// get local node from igp_node entry

	ln, err := a.getLocalV6node(ctx, p, true)
	if err != nil {
		glog.Errorf("processEdge failed to get local peer %s for link: %s with error: %+v", p.LocalBGPID, p.ID, err)
		return err
	}

	// get remote node from ebgp peer entry
	rn, err := a.getExternalV6Node(ctx, p, false)
	if err != nil {
		glog.Errorf("processEdge failed to get remote peer %s for link: %s with error: %+v", p.RemoteBGPID, p.ID, err)
		return err
	}
	if err := a.createPRV6edge(ctx, p, ln, rn); err != nil {
		glog.Errorf("processEdge failed to create Edge object with error: %+v", err)
		return err
	}
	glog.Infof("processEdge completed processing eBGP peer: %s for ls node: %s - %s", p.ID, ln.ID, rn.ID)
	return nil
}

func (a *arangoDB) processAsbrV6(ctx context.Context, key string, p *message.PeerStateChange) error {

	glog.Infof("process ebgp session: %s", p.Key)
	// get local node from igp_node entry

	ln, err := a.getLocalV6node(ctx, p, true)
	if err != nil {
		glog.Errorf("processEdge failed to get local peer %s for link: %s with error: %+v", p.LocalBGPID, p.ID, err)
		return err
	}

	// get remote node from igp_node entry
	rn, err := a.getRemoteV6Node(ctx, p, false)
	if err != nil {
		glog.Errorf("processEdge failed to get remote peer %s for link: %s with error: %+v", p.RemoteBGPID, p.ID, err)
		return err
	}
	if err := a.createAsbrV6edge(ctx, p, ln, rn); err != nil {
		glog.Errorf("processEdge failed to create Edge object with error: %+v", err)
		return err
	}
	glog.Infof("processEdge completed processing eBGP peer: %s for ls node: %s - %s", p.ID, ln.ID, rn.ID)
	return nil
}

// getLocalnode returns the inside or ls_node side of the eBGP session. the || d.bgp_router ipv6-only ls_nodes
func (a *arangoDB) getLocalV6node(ctx context.Context, e *message.PeerStateChange, local bool) (igpNode, error) {
	// Need to find ls_node object matching ls_link's IGP Router ID
	query := "FOR d IN " + a.igpNode.Name()
	if local {
		glog.Infof("get local node per session: %s, %s", e.LocalBGPID, e.ID)
		query += " filter d.router_id == " + "\"" + e.LocalBGPID + "\"" + " || d.bgp_router_id == " + "\"" + e.LocalBGPID + "\""
	} else {
		glog.Infof("get remote node per session: %s, %v", e.RemoteBGPID, e.ID)
		query += " filter d.router_id == " + "\"" + e.RemoteBGPID + "\"" + " || d.bgp_router_id == " + "\"" + e.RemoteBGPID + "\""
	}
	query += " return d"
	glog.Infof("query: %+v", query)
	lcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		glog.Errorf("failed to process key: %s with error: %+v", e.Key, err)
	}
	defer lcursor.Close()
	var ln igpNode
	i := 0
	for ; ; i++ {
		_, err := lcursor.ReadDocument(ctx, &ln)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				glog.Errorf("failed to process key: %s with error: %+v", e.Key, err)
			}
			break
		}
	}
	if i == 0 {
		glog.Errorf("query %s returned 0 results", query)
	}
	if i > 1 {
		glog.Errorf("query %s returned more than 1 result", query)
	}
	return ln, nil
}

// getExtertalV6Node returns the outside or ebgp peer side of the lsnode to eBGP session. the || d.bgp_router_id is for ipv6-only lsnodes
func (a *arangoDB) getExternalV6Node(ctx context.Context, e *message.PeerStateChange, local bool) (bgpNode, error) {
	// Need to find ls_node object matching ls_link's IGP Router ID
	query := "FOR d IN " + a.bgpNode.Name()
	if local {
		glog.Infof("get local node per session: %s, %s", e.LocalBGPID, e.ID)
		query += " filter d.router_id == " + "\"" + e.LocalBGPID + "\"" + " || d.bgp_router_id == " + "\"" + e.LocalBGPID + "\""
	} else {
		glog.Infof("get remote node per session: %s, %v", e.RemoteBGPID, e.ID)
		query += " filter d.router_id == " + "\"" + e.RemoteBGPID + "\"" + " || d.bgp_router_id == " + "\"" + e.RemoteBGPID + "\""
	}
	query += " return d"
	//glog.Infof("query: %+v", query)
	lcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		glog.Errorf("failed to process key: %s with error: %+v", e.Key, err)
	}
	defer lcursor.Close()
	var ln bgpNode
	i := 0
	for ; ; i++ {
		_, err := lcursor.ReadDocument(ctx, &ln)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				glog.Errorf("failed to process key: %s with error: %+v", e.Key, err)
			}
			break
		}
	}
	if i == 0 {
		glog.Errorf("query %s returned 0 results", query)
	}
	if i > 1 {
		glog.Errorf("query %s returned more than 1 result", query)
	}
	return ln, nil
}

// getExtPeer returns the outside or ebgp peer side of the lsnode to eBGP session. the || d.bgp_router_id is for ipv6-only lsnodes
func (a *arangoDB) getRemoteV6Node(ctx context.Context, e *message.PeerStateChange, local bool) (igpNode, error) {
	// Need to find ls_node object matching ls_link's IGP Router ID
	query := "FOR d IN " + a.igpNode.Name()
	if local {
		glog.Infof("get local node per session: %s, %s", e.LocalBGPID, e.ID)
		query += " filter d.router_id == " + "\"" + e.LocalBGPID + "\"" + " || d.bgp_router_id == " + "\"" + e.LocalBGPID + "\""
	} else {
		glog.Infof("get remote node per session: %s, %v", e.RemoteBGPID, e.ID)
		query += " filter d.router_id == " + "\"" + e.RemoteBGPID + "\"" + " || d.bgp_router_id == " + "\"" + e.RemoteBGPID + "\""
	}
	query += " return d"
	//glog.Infof("query: %+v", query)
	lcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		glog.Errorf("failed to process key: %s with error: %+v", e.Key, err)
	}
	defer lcursor.Close()
	var ln igpNode
	i := 0
	for ; ; i++ {
		_, err := lcursor.ReadDocument(ctx, &ln)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				glog.Errorf("failed to process key: %s with error: %+v", e.Key, err)
			}
			break
		}
	}
	if i == 0 {
		glog.Errorf("query %s returned 0 results", query)
	}
	if i > 1 {
		glog.Errorf("query %s returned more than 1 result", query)
	}
	return ln, nil
}

func (a *arangoDB) createPRV6edge(ctx context.Context, p *message.PeerStateChange, ln igpNode, rn bgpNode) error {

	if p.LocalASN == p.RemoteASN {
		glog.Infof("peer message is iBGP, no further processing")
	} else {
		pf := peerEdge{
			Key:       ln.Key + "_" + p.Key,
			From:      ln.ID,
			To:        rn.ID,
			LocalIP:   p.LocalIP,
			RemoteIP:  p.RemoteIP,
			LocalASN:  p.LocalASN,
			RemoteASN: p.RemoteASN,
		}
		if _, err := a.ipv6Edge.CreateDocument(ctx, &pf); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.ipv6Edge.UpdateDocument(ctx, pf.Key, &pf); err != nil {
				return err
			}
		}
		pt := peerEdge{
			Key:       rn.Key + "_" + p.Key,
			From:      rn.ID,
			To:        ln.ID,
			Session:   p.Key,
			LocalIP:   p.LocalIP,
			RemoteIP:  p.RemoteIP,
			LocalASN:  p.LocalASN,
			RemoteASN: p.RemoteASN,
		}
		if _, err := a.ipv6Edge.CreateDocument(ctx, &pt); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.ipv6Edge.UpdateDocument(ctx, pt.Key, &pt); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *arangoDB) createAsbrV6edge(ctx context.Context, p *message.PeerStateChange, ln igpNode, rn igpNode) error {

	if p.LocalASN == p.RemoteASN {
		glog.Infof("peer message is iBGP, no further processing")
	} else {
		pf := asbrEdge{
			Key:               ln.Key + "_" + p.Key,
			From:              ln.ID,
			To:                rn.ID,
			Name:              ln.Name + "_to_" + rn.Name,
			LocalIGPRouterID:  ln.RouterID,
			RemoteIGPRouterID: rn.RouterID,
			LocalASN:          ln.ASN,
			RemoteASN:         rn.ASN,
		}
		if _, err := a.ipv6Edge.CreateDocument(ctx, &pf); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.ipv6Edge.UpdateDocument(ctx, pf.Key, &pf); err != nil {
				return err
			}
		}
		pt := asbrEdge{
			Key:               rn.Key + "_" + p.Key,
			From:              rn.ID,
			To:                ln.ID,
			Name:              rn.Name + "_to_" + ln.Name,
			LocalIGPRouterID:  rn.RouterID,
			RemoteIGPRouterID: ln.RouterID,
			LocalASN:          rn.ASN,
			RemoteASN:         ln.ASN,
		}
		if _, err := a.ipv6Edge.CreateDocument(ctx, &pt); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.ipv6Edge.UpdateDocument(ctx, pt.Key, &pt); err != nil {
				return err
			}
		}
	}
	return nil
}
