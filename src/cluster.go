// Copyright 2016 Eric Mortensen. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package loadbalancer

import (
	"database/sql"
	"regexp"
)

// Cluster maintains a cluster of nodes, each of which represents a host participating in load balancing.
type Cluster struct {
	// The nodes participating in this cluster
	Nodes []*Node

	lb LoadBalancer

	// Maximum number of idle connections on this host
	MaxIdleConnections int
}

// Main structure for a node participating in a cluster.
type Node struct {
	// The URL used to connect to this node.
	Url     string
	RegEx   regexp.Regexp
	db_     *sql.DB
	cluster *Cluster

	// The type of node, e.g., "postgres", "mysql" etc.
	Type   string
	values map[string]string

	// A mode indicates what type of node this is. Used by a LoadBalancer to determine which node to use
	// For example, for MultiReaderSingleWriter load balancers, Mode can be either Read or Write.
	Mode int16
}

// Context represents a client context when Acquire-ing connections.
type Context struct {
	// The mode the client wants. Used by e.g., MultiReaderSingleWriter load balancers to choose between Read or Write nodes
	Mode int16

	// The Sql the client intends to send. Used by e.g., ParsingMultiReaderSingleWriterBalancer to parse the Sql to
	// determine if the Sql is read-only or read-write
	Sql    string
	values map[string]string
}

func (ctx *Context) GetValue(key string) string {
	if ctx.values == nil {
		return ""
	}
	return ctx.values[key]
}

func (node *Node) GetValue(key string) string {
	if node.values == nil {
		return ""
	}
	return node.values[key]
}

func (node *Node) db() (*sql.DB, error) {
	if node.db_ == nil {
		dburl := node.Url
		var err error
		node.db_, err = sql.Open(node.Type, dburl)
		if err != nil {
			if node.db_ != nil {
				node.db_.Close()
			}
			return nil, err
		}
		if node.cluster.MaxIdleConnections != 0 {
			node.db_.SetMaxIdleConns(node.cluster.MaxIdleConnections)
		}
	}
	return node.db_, nil
}

// Add adds a node to the cluster.
func (c *Cluster) Add(node *Node) {
	if c.Nodes == nil {
		c.Nodes = make([]*Node, 0)
	}
	c.Nodes = append(c.Nodes, node)
	node.cluster = c

	if c.lb == nil {
		c.SetLoadBalancer(&RandomLoadBalancer{})
	}
	c.lb.Add(node)
}

// SetLoadBalancer sets the load balancer strategy for this cluster
func (c *Cluster) SetLoadBalancer(lb LoadBalancer) {
	c.lb = lb
	lb.SetCluster(c)
}

// Acquire acquires a new database connection from the cluster, depending on the context and the cluster's
// load balancer strategy.
func (c *Cluster) Acquire(context *Context) (*sql.DB, error) {
	return c.lb.Acquire(context)
}
