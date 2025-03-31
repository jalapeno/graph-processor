# Jalapeno Graph Processor

## Description

graph-processor is a Jalapeno data processor service that compiles existing IGP and BGP data from Arango document/key-value collections and creates four graph collections:

* igpv4_graph - a graphDB model of the IGP's IPv4 topology
* igpv6_graph - a graphDB model of the IGP's IPv6 topology
* ipv4_graph - a graphDB model of the entire IPv4 topology 
* ipv6_graph - a graphDB model of the entire IPv6 topology

The processor also subcribes to GoBMP events topics on Kafka and inserts or deletes entries in the graph collections accordingly.

## Prerequisites
* Kubernetes cluster
* Jalapeno platform installed [Jalapeno Platform](https://github.com/cisco-open/jalapeno)

