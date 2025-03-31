package arangodb

import (
	"context"
	"fmt"
	"strconv"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

// eBGP private
func (a *arangoDB) processPeerV6Session(ctx context.Context, key string, p *message.PeerStateChange) error {
	//glog.Infof("process bgp session: %s", p.Key)

	if p.IsIPv4 == true {
		return nil
	} else {
		ln, err := a.getPeerV6(ctx, p, true)
		if err != nil {
			glog.Errorf("processEdge failed to get local peer %s for link: %s with error: %+v", p.LocalBGPID, p.ID, err)
			return err
		}
		// get remote node from peer entry
		rn, err := a.getPeerV6(ctx, p, false)
		if err != nil {
			glog.Errorf("processEdge failed to get remote peer %s for link: %s with error: %+v", p.RemoteBGPID, p.ID, err)
			return err
		}
		if err := a.createPeerV6Edge(ctx, p, ln, rn); err != nil {
			glog.Errorf("processEdge failed to create Edge object with error: %+v", err)
			return err
		}
	}
	//glog.V(9).Infof("processEdge completed processing lslink: %s for ls nodes: %s - %s", l.ID, ln.ID, rn.ID)
	return nil
}

func (a *arangoDB) getPeerV6(ctx context.Context, e *message.PeerStateChange, local bool) (bgpNode, error) {
	// Need to find ls_node object matching ls_link's IGP Router ID
	query := "FOR d IN " + a.bgpNode.Name()
	if local {
		//glog.Infof("get local node per session: %s, %s", e.LocalBGPID, e.ID)
		query += " filter d.router_id == " + "\"" + e.LocalBGPID + "\""
	} else {
		//glog.Infof("get remote node per session: %s, %v", e.RemoteBGPID, e.ID)
		query += " filter d.router_id == " + "\"" + e.RemoteBGPID + "\""
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

func (a *arangoDB) createPeerV6Edge(ctx context.Context, l *message.PeerStateChange, ln, rn bgpNode) error {
	if a == nil || a.ipv6Edge == nil {
		return fmt.Errorf("invalid arangoDB instance or graph is nil")
	}

	//glog.Infof("create peer edge for: %s, with local node: %s and remote node: %s", l.Key, ln.ID, rn.ID)
	pf := peerEdge{
		//Key:       l.Key,
		Key:       l.RemoteBGPID + "_" + strconv.Itoa(int(l.RemoteASN)) + "_" + l.RemoteIP,
		From:      ln.ID,
		To:        rn.ID,
		LocalIP:   l.LocalIP,
		RemoteIP:  l.RemoteIP,
		LocalASN:  l.LocalASN,
		RemoteASN: l.RemoteASN,
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
	return nil
}
