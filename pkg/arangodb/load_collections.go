package arangodb

import (
	"context"
	"fmt"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

// loadCollections calls a series of subfunctions to perform ArangoDB operations including populating
// ls_node_extended collection and querying other link state collections to build the igpv4_graph and igpv6_graph
func (a *arangoDB) loadCollections() error {
	ctx := context.TODO()

	if err := a.igpNodes(ctx); err != nil {
		return err
	}
	if err := a.processDuplicateNodes(ctx); err != nil {
		return err
	}
	if err := a.loadPrefixSIDs(ctx); err != nil {
		return err
	}
	if err := a.loadSRv6SIDs(ctx); err != nil {
		return err
	}
	if err := a.processIBGPv6Peering(ctx); err != nil {
		return err
	}
	if err := a.createIGPDomains(ctx); err != nil {
		return err
	}
	if err := a.igpv4LinkEdges(ctx); err != nil {
		return err
	}
	if err := a.igpv4PrefixEdges(ctx); err != nil {
		return err
	}
	if err := a.igpv6LinkEdges(ctx); err != nil {
		return err
	}
	if err := a.igpv6PrefixEdges(ctx); err != nil {
		return err
	}

	if err := a.loadEdgeV4(); err != nil {
		return err
	}
	if err := a.loadEdgeV6(); err != nil {
		return err
	}
	return nil
}

func (a *arangoDB) igpNodes(ctx context.Context) error {
	// Build AQL query with proper OPTIONS clause to handle duplicates
	query := fmt.Sprintf(
		"FOR l IN %s INSERT l IN %s OPTIONS { ignoreErrors: true }",
		a.lsnode.Name(),
		a.igpNode.Name(),
	)

	cursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("failed to execute igpNodes query: %w", err)
	}
	defer cursor.Close()

	// Wait for all documents to be processed
	for cursor.HasMore() {
		if _, err := cursor.ReadDocument(ctx, nil); err != nil {
			return fmt.Errorf("error reading cursor: %w", err)
		}
	}

	return nil
}

func (a *arangoDB) processDuplicateNodes(ctx context.Context) error {
	// BGP-LS generates both a level-1 and a level-2 entry for level-1-2 nodes
	// Here we remove duplicate entries in the igp_node collection
	dup_query := fmt.Sprintf(`
		LET duplicates = (
			FOR d IN %s
			COLLECT id = d.igp_router_id, 
					domain = d.domain_id, 
					area = d.area_id 
			WITH COUNT INTO count
			FILTER count > 1
			RETURN { 
				id: id, 
				domain: domain, 
				area: area, 
				count: count 
			}
		)
		FOR d IN duplicates 
		FOR m IN %s
		FILTER d.id == m.igp_router_id 
		FILTER d.domain == m.domain_id 
		RETURN m`,
		a.igpNode.Name(),
		a.igpNode.Name(),
	)

	pcursor, err := a.db.Query(ctx, dup_query, nil)
	if err != nil {
		return fmt.Errorf("failed to execute duplicate nodes query: %w", err)
	}
	defer pcursor.Close()

	for {
		var doc duplicateNode
		meta, err := pcursor.ReadDocument(ctx, &doc)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return fmt.Errorf("error reading document: %w", err)
		}

		glog.V(5).Infof("Processing duplicate node with key '%s'", meta.Key)

		switch doc.ProtocolID {
		case 1: // ISIS Level-1
			glog.V(5).Infof("Removing level-1 duplicate node: key=%s, igp_id=%s, protocol_id=%d",
				doc.Key, doc.IGPRouterID, doc.ProtocolID)

			if _, err := a.igpNode.RemoveDocument(ctx, doc.Key); err != nil {
				if !driver.IsConflict(err) {
					return fmt.Errorf("failed to remove duplicate node: %w", err)
				}
				glog.V(5).Infof("Conflict while removing document %s, continuing", doc.Key)
			}

		case 2: // ISIS Level-2
			update_query := fmt.Sprintf(`
				FOR l IN %s
				FILTER l._key == @key
				UPDATE l WITH { 
					protocol: "ISIS Level 1-2" 
				} IN %s`,
				a.igpNode.Name(),
				a.igpNode.Name(),
			)

			bindVars := map[string]interface{}{
				"key": doc.Key,
			}

			cursor, err := a.db.Query(ctx, update_query, bindVars)
			if err != nil {
				return fmt.Errorf("failed to update node protocol: %w", err)
			}
			glog.V(5).Infof("Updated node %s to ISIS Level 1-2", doc.Key)
			cursor.Close()
		}
	}

	return nil
}

func (a *arangoDB) loadPrefixSIDs(ctx context.Context) error {
	// Find and add sr-mpls prefix sids to nodes in the igp_node collection
	sr_query := fmt.Sprintf(`
		FOR p IN %s 
		FILTER p.mt_id_tlv.mt_id != 2 
		FILTER p.prefix_attr_tlvs.ls_prefix_sid != null 
		RETURN p`,
		a.lsprefix.Name(),
	)
	cursor, err := a.db.Query(ctx, sr_query, nil)
	if err != nil {
		return fmt.Errorf("failed to execute prefix SIDs query: %w", err)
	}
	defer cursor.Close()
	for {
		var p message.LSPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return fmt.Errorf("error reading document: %w", err)
		}
		if err := a.processPrefixSID(ctx, meta.Key, meta.ID.String(), p); err != nil {
			glog.Errorf("Failed to process ls_prefix_sid %s with error: %+v", p.ID, err)
		}
	}

	return nil
}

func (a *arangoDB) loadSRv6SIDs(ctx context.Context) error {
	// Find and add srv6 sids to nodes in the igp_node collection
	srv6_query := fmt.Sprintf(`
		FOR s IN %s 
		RETURN s`,
		a.lssrv6sid.Name(),
	)
	cursor, err := a.db.Query(ctx, srv6_query, nil)
	if err != nil {
		return fmt.Errorf("failed to execute SRv6 SIDs query: %w", err)
	}
	defer cursor.Close()
	for {
		var p message.LSSRv6SID
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return fmt.Errorf("error reading document: %w", err)
		}
		if err := a.processLSSRv6SID(ctx, meta.Key, meta.ID.String(), &p); err != nil {
			glog.Errorf("Failed to process ls_srv6_sid %s with error: %+v", p.ID, err)
		}
	}

	return nil
}

func (a *arangoDB) processIBGPv6Peering(ctx context.Context) error {
	// add ipv6 iBGP peering address and ipv4 bgp router-id
	glog.Infof("processing IBGPv6 peering")
	ibgp6_query := fmt.Sprintf(`
		FOR s IN peer 
		FILTER s.remote_ip LIKE "%%:%%"
		RETURN s`,
	)
	cursor, err := a.db.Query(ctx, ibgp6_query, nil)
	if err != nil {
		return fmt.Errorf("failed to execute IBGPv6 peering query: %w", err)
	}
	defer cursor.Close()
	for {
		var p message.PeerStateChange
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return fmt.Errorf("error reading document: %w", err)
		}
		if err := a.processbgp6(ctx, meta.Key, meta.ID.String(), &p); err != nil {
			glog.Errorf("Failed to process ibgp peering %s with error: %+v", p.ID, err)
		}
	}
	glog.Infof("completed processing IBGPv6 peering")
	return nil
}

func (a *arangoDB) createIGPDomains(ctx context.Context) error {
	// create igp_domain collection - useful in scaled multi-domain environments
	glog.Infof("creating IGP domains")
	igpdomain_query := "for l in igp_node insert " +
		"{ _key: CONCAT_SEPARATOR(" + "\"_\", l.protocol_id, l.domain_id, l.asn), " +
		"asn: l.asn, protocol_id: l.protocol_id, domain_id: l.domain_id, protocol: l.protocol } " +
		"into igp_domain OPTIONS { ignoreErrors: true } return l"
	cursor, err := a.db.Query(ctx, igpdomain_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	return nil
}

func (a *arangoDB) igpv4LinkEdges(ctx context.Context) error {
	// Find ipv4 ls_link entries to create edges in the igpv4_graph
	igpv4linkquery := fmt.Sprintf(`
		FOR l IN %s 
		FILTER l.protocol_id != 7 
		RETURN l`,
		a.lslink.Name(),
	)
	cursor, err := a.db.Query(ctx, igpv4linkquery, nil)
	if err != nil {
		return fmt.Errorf("failed to execute IGPv4 link edges query: %w", err)
	}
	defer cursor.Close()
	for {
		var p message.LSLink
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return fmt.Errorf("error reading document: %w", err)
		}
		if err := a.processLSLinkEdge(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	return nil
}

func (a *arangoDB) igpv4PrefixEdges(ctx context.Context) error {
	// Find ls_prefix entries to create prefix or subnet edges in the igpv4_graph
	igpv4pfxquery := fmt.Sprintf(`
		FOR l IN %s
		FILTER l.mt_id_tlv.mt_id != 2 
		FILTER l.prefix_len != 30 
		FILTER l.prefix_len != 31 
		FILTER l.prefix_len != 32 
		RETURN l`,
		a.lsprefix.Name(),
	)
	cursor, err := a.db.Query(ctx, igpv4pfxquery, nil)
	if err != nil {
		return fmt.Errorf("failed to execute IGPv4 prefix edges query: %w", err)
	}
	defer cursor.Close()
	for {
		var p message.LSPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return fmt.Errorf("error reading document: %w", err)
		}
		if err := a.processLSPrefixEdge(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	return nil
}

func (a *arangoDB) igpv6LinkEdges(ctx context.Context) error {
	// Find ipv6 ls_link entries to create edges in the igpv6_graph
	igpv6linkquery := fmt.Sprintf(`
		FOR l IN %s 
		FILTER l.protocol_id != 7 
		RETURN l`,
		a.lslink.Name(),
	)
	cursor, err := a.db.Query(ctx, igpv6linkquery, nil)
	if err != nil {
		return fmt.Errorf("failed to execute IGPv6 link edges query: %w", err)
	}
	defer cursor.Close()
	for {
		var p message.LSLink
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return fmt.Errorf("error reading document: %w", err)
		}
		if err := a.processigpv6LinkEdge(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	return nil
}

func (a *arangoDB) igpv6PrefixEdges(ctx context.Context) error {
	// Find ipv6 ls_prefix entries to create prefix or subnet edges in the igpv6_graph
	igpv6pfxquery := fmt.Sprintf(`
		FOR l IN %s
		FILTER l.mt_id_tlv.mt_id == 2 
		FILTER l.prefix_len != 126 
		FILTER l.prefix_len != 127 
		FILTER l.prefix_len != 128 
		RETURN l`,
		a.lsprefix.Name(),
	)
	cursor, err := a.db.Query(ctx, igpv6pfxquery, nil)
	if err != nil {
		return fmt.Errorf("failed to execute IGPv6 prefix edges query: %w", err)
	}
	defer cursor.Close()
	for {
		var p message.LSPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return fmt.Errorf("error reading document: %w", err)
		}
		if err := a.processigpv6PrefixEdge(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	return nil
}

// Start loading vertices and edges into the graph
func (a *arangoDB) loadEdgeV4() error {
	ctx := context.TODO()
	glog.Infof("start processing vertices and edges")

	glog.Infof("insert link-state graph topology into ipv4 graph")
	copy_ls_topo := "for l in igpv4_graph insert l in ipv4_graph options { overwrite: " + "\"update\"" + " } "
	cursor, err := a.db.Query(ctx, copy_ls_topo, nil)
	if err != nil {
		glog.Errorf("Failed to copy link-state topology; it may not exist or have been populated in the database: %v", err)
	} else {
		defer cursor.Close()
	}

	//glog.Infof("copying private ASN ebgp unicast v4 prefixes into ebgp_prefix_v4 collection")
	ebgp4_query := "FOR u IN unicast_prefix_v4 FILTER u.peer_asn IN 64512..65535 FILTER u.origin_as IN 64512..65535 " +
		"FILTER u.prefix_len < 30 FILTER u.base_attrs.as_path_count == 1 FOR p IN peer FILTER u.peer_ip == p.remote_ip " +
		"INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", u.prefix, u.prefix_len), prefix: u.prefix, prefix_len: u.prefix_len, " +
		"origin_as: u.origin_as, nexthop: u.nexthop, peer_ip: u.peer_ip, remote_ip: p.remote_ip, router_id: p.remote_bgp_id } " +
		"INTO ebgp_prefix_v4 OPTIONS { ignoreErrors: true } "
	cursor, err = a.db.Query(ctx, ebgp4_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	//glog.Infof("copying public ASN unicast v4 prefixes into inet_prefix_v4 collection")
	inet4_query := "for u in unicast_prefix_v4 let internal_asns = ( for l in ls_node return l.peer_asn ) " +
		"filter u.peer_asn not in internal_asns filter u.peer_asn !in 64512..65535 filter u.origin_as !in 64512..65535 filter u.prefix_len < 96 " +
		"filter u.remote_asn != u.origin_as INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", u.prefix, u.prefix_len)," +
		"prefix: u.prefix, prefix_len: u.prefix_len, origin_as: u.origin_as, nexthop: u.nexthop } " +
		"INTO inet_prefix_v4 OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, inet4_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// iBGP goes last as we sort out which prefixes are orginated externally
	//glog.Infof("copying ibgp unicast v4 prefixes into ibgp_prefix_v4 collection")
	ibgp4_query := "for u in unicast_prefix_v4 FILTER u.prefix_len < 30 filter u.base_attrs.local_pref != null " +
		"FILTER u.prefix_len < 30 FILTER u.base_attrs.as_path_count == null FOR p IN peer FILTER u.peer_ip == p.remote_ip " +
		"INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", u.prefix, u.prefix_len), prefix: u.prefix, prefix_len: u.prefix_len, " +
		"nexthop: u.nexthop, router_id: p.remote_bgp_id, asn: u.peer_asn, local_pref: u.base_attrs.local_pref } " +
		"INTO ibgp_prefix_v4 OPTIONS { ignoreErrors: true } "
	cursor, err = a.db.Query(ctx, ibgp4_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	glog.Infof("copying unique bgp_node into bgp_node collection")
	bgp_node_query := "for p in peer let igp_asns = ( for n in igp_node return n.peer_asn ) " +
		"filter p.remote_asn not in igp_asns " +
		"insert { _key: CONCAT_SEPARATOR(" + "\"_\", p.remote_bgp_id, p.remote_asn), " +
		"router_id: p.remote_bgp_id, asn: p.remote_asn  } INTO bgp_node OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, bgp_node_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// start building ipv4 graph
	peer2peer_query := "for p in peer return p"
	cursor, err = a.db.Query(ctx, peer2peer_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.PeerStateChange
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("find ebgp peers to populate graph: %s", p.Key)
		if err := a.processPeerSession(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	bgp_prefix_query := "for p in ebgp_prefix_v4 return p"
	cursor, err = a.db.Query(ctx, bgp_prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p bgpPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("get ipv eBGP prefixes: %s", p.Key)
		if err := a.processeBgpV4Prefix(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	inet_prefix_query := "for p in inet_prefix_v4 return p"
	cursor, err = a.db.Query(ctx, inet_prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.UnicastPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("get ipv inet prefixes: %s", p.Key)
		if err := a.processInetV4Prefix(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	ibgp_prefix_query := "for p in ibgp_prefix_v4 return p"
	cursor, err = a.db.Query(ctx, ibgp_prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p ibgpPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("get ipv ibgp prefixes: %s", p.Key)
		if err := a.processIbgpPrefix(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	// Find eBGP egress / Inet peers from IGP domain. This could also be egress from IGP domain to internal eBGP peers
	bgp_query := "for l in peer let internal_asns = ( for n in igp_node return n.peer_asn ) " +
		"filter l.local_asn in internal_asns && l.remote_asn not in internal_asns filter l._key like " + "\"%:%\"" + " return l"
	cursor, err = a.db.Query(ctx, bgp_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.PeerStateChange
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("processing eBGP peers for ls_node: %s", p.Key)
		if err := a.processEgressPeer(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	// Find BGP sessions for ASBR nodes at the edge of IGP domains
	asbr_query := "for l in peer let internal_asns = ( for n in igp_node return n.peer_asn ) " +
		"filter l.local_asn in internal_asns && l.remote_asn in internal_asns " +
		" filter l.local_asn != l.remote_asn filter l._key !like " + "\"%:%\"" + " return l"
	cursor, err = a.db.Query(ctx, asbr_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.PeerStateChange
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		glog.Infof("processing eBGP peers for ls_node: %s", p.Key)
		if err := a.processAsbrV4(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	return nil
}

// ipv6 load collections:

// Start loading vertices and edges into the graph
func (a *arangoDB) loadEdgeV6() error {
	ctx := context.TODO()
	glog.Infof("start processing vertices and edges")

	glog.Infof("insert link-state graph topology into ipv6 graph")
	copy_ls_topo := "for l in igpv6_graph insert l in ipv6_graph options { overwrite: " + "\"update\"" + " } "
	cursor, err := a.db.Query(ctx, copy_ls_topo, nil)
	if err != nil {
		glog.Errorf("Failed to copy link-state topology; it may not exist or have been populated in the database: %v", err)
	} else {
		defer cursor.Close()
	}

	//glog.Infof("copying private ASN ebgp unicast v6 prefixes into ebgp_prefix_v6 collection")
	ebgp6_query := "FOR u IN unicast_prefix_v6 FILTER u.peer_asn IN 64512..65535 FILTER u.origin_as IN 64512..65535 " +
		"FILTER u.prefix_len < 96 FILTER u.base_attrs.as_path_count == 1 FOR p IN peer FILTER u.peer_ip == p.remote_ip " +
		"INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", u.prefix, u.prefix_len), prefix: u.prefix, prefix_len: u.prefix_len, " +
		"origin_as: u.origin_as, nexthop: u.nexthop, peer_ip: u.peer_ip, remote_ip: p.remote_ip, router_id: p.remote_bgp_id } " +
		"INTO ebgp_prefix_v6 OPTIONS { ignoreErrors: true } "
	cursor, err = a.db.Query(ctx, ebgp6_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	//glog.Infof("copying 4-byte private ASN ebgp unicast v6 prefixes into ebgp_prefix_v6 collection")
	fourByteEbgp6Query := "FOR u IN unicast_prefix_v6 FILTER u.peer_asn IN 4200000000..4294967294 " +
		"FILTER u.prefix_len < 96 FILTER u.base_attrs.as_path_count == 1 FOR p IN peer FILTER u.peer_ip == p.remote_ip " +
		"INSERT { _key: CONCAT_SEPARATOR(\"_\", u.prefix, u.prefix_len), prefix: u.prefix, prefix_len: u.prefix_len, " +
		"origin_as: u.origin_as < 0 ? u.origin_as + 4294967296 : u.origin_as, nexthop: u.nexthop, peer_ip: u.peer_ip, " +
		"remote_ip: p.remote_ip, router_id: p.remote_bgp_id } " +
		"INTO ebgp_prefix_v6 OPTIONS { ignoreErrors: true } "
	cursor, err = a.db.Query(ctx, fourByteEbgp6Query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	//glog.Infof("copying public ASN unicast v6 prefixes into inet_prefix_v6 collection")
	inet6_query := "for u in unicast_prefix_v6 let internal_asns = ( for l in ls_node return l.peer_asn ) " +
		"filter u.peer_asn not in internal_asns filter u.peer_asn !in 64512..65535 filter u.peer_asn !in  4200000000..4294967294 " +
		" filter u.origin_as !in 64512..65535 filter u.prefix_len < 96 " +
		"filter u.remote_asn != u.origin_as INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", u.prefix, u.prefix_len)," +
		"prefix: u.prefix, prefix_len: u.prefix_len, origin_as: u.origin_as, nexthop: u.nexthop } " +
		"INTO inet_prefix_v6 OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, inet6_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// iBGP goes last as we sort out which prefixes are orginated externally
	//glog.Infof("copying ibgp unicast v6 prefixes into ibgp_prefix_v6 collection")
	ibgp6_query := "for u in unicast_prefix_v6 FILTER u.prefix_len < 96 filter u.base_attrs.local_pref != null " +
		"FILTER u.prefix_len < 96 FILTER u.base_attrs.as_path_count == null FOR p IN peer FILTER u.peer_ip == p.remote_ip " +
		"INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", u.prefix, u.prefix_len), prefix: u.prefix, prefix_len: u.prefix_len, " +
		"nexthop: u.nexthop, router_id: p.remote_bgp_id, asn: u.peer_asn, local_pref: u.base_attrs.local_pref } " +
		"INTO ibgp_prefix_v6 OPTIONS { ignoreErrors: true } "
	cursor, err = a.db.Query(ctx, ibgp6_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	glog.Infof("copying unique ebgp peers into bgp_node collection")
	ebgp_peer_query := "for p in peer let igp_asns = ( for n in igp_node return n.peer_asn ) " +
		"filter p.remote_asn not in igp_asns " +
		"insert { _key: CONCAT_SEPARATOR(" + "\"_\", p.remote_bgp_id, p.remote_asn), " +
		"router_id: p.remote_bgp_id, asn: p.remote_asn  } INTO bgp_node OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, ebgp_peer_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// start building ipv6 graph
	peer2peer_query := "for p in peer return p"
	cursor, err = a.db.Query(ctx, peer2peer_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.PeerStateChange
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("find ebgp peers to populate graph: %s", p.Key)
		if err := a.processPeerV6Session(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	bgp_prefix_query := "for p in ebgp_prefix_v6 return p"
	cursor, err = a.db.Query(ctx, bgp_prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p bgpPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("get ipv6 eBGP prefixes: %s", p.Key)
		if err := a.processeBgpV6Prefix(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	inet_prefix_query := "for p in inet_prefix_v6 return p"
	cursor, err = a.db.Query(ctx, inet_prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.UnicastPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("get ipv6 inet prefixes: %s", p.Key)
		if err := a.processInetV6Prefix(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	glog.Infof("get ipv6 ibgp prefixes")
	ibgp_prefix_query := "for p in ibgp_prefix_v6 return p"
	cursor, err = a.db.Query(ctx, ibgp_prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p ibgpPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		if err := a.processIbgpV6Prefix(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	// Find eBGP egress / Inet peers from IGP domain. This could also be egress from IGP domain to internal eBGP peers
	bgp_query := "for l in peer let internal_asns = ( for n in igp_node return n.peer_asn ) " +
		"filter l.local_asn in internal_asns && l.remote_asn not in internal_asns filter l._key like " + "\"%:%\"" + " return l"
	cursor, err = a.db.Query(ctx, bgp_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.PeerStateChange
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		glog.Infof("processing eBGP peers for ls_node: %s", p.Key)
		if err := a.processEgressV6Peer(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	// Find BGP sessions for ASBR nodes at the edge of IGP domains
	asbr_query := "for l in peer let internal_asns = ( for n in igp_node return n.peer_asn ) " +
		"filter l.local_asn in internal_asns && l.remote_asn in internal_asns " +
		" filter l.local_asn != l.remote_asn filter l._key like " + "\"%:%\"" + " return l"
	cursor, err = a.db.Query(ctx, asbr_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.PeerStateChange
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		glog.Infof("processing eBGP peers for ls_node: %s", p.Key)
		if err := a.processAsbrV6(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	return nil
}
