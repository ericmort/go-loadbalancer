// Copyright 2016 Eric Mortensen. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package loadbalancer

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))
}

// LoadBalancer represents a load balancing stragegy
type LoadBalancer interface {

	// Acquire a database connection using this load balancing strategy
	Acquire(context *Context) (*sql.DB, error)

	// Add the node to the load balancer
	Add(node *Node)

	// Set the cluster for this load balancer
	SetCluster(c *Cluster)
}

// RandomLoadBalancer represents a uniformly random strategy of picking nodes
type RandomLoadBalancer struct {
	LoadBalancer
	cluster *Cluster
}

func (rlb *RandomLoadBalancer) Add(node *Node) {
}

func (rlb *RandomLoadBalancer) Acquire(ctx *Context) (*sql.DB, error) {
	i := rand.Int31n(int32(len(rlb.cluster.Nodes)))
	return rlb.cluster.Nodes[i].db()
}

func (rlb *RandomLoadBalancer) SetCluster(c *Cluster) {
	rlb.cluster = c
}

type MultiReaderSingleWriterBalancer struct {
	LoadBalancer
	readers []*Node
	writer  *Node
}

type SqlParser interface {
	Parse(string) int16
}

type ParsingMultiReaderSingleWriterBalancer struct {
	MultiReaderSingleWriterBalancer
	SqlParser
}

const (
	Read  = 0
	Write = 1
)

func (rlb *MultiReaderSingleWriterBalancer) Add(node *Node) {
	if rlb.readers == nil {
		rlb.readers = make([]*Node, 0)
	}
	switch node.Mode {
	case Read:
		rlb.readers = append(rlb.readers, node)
	case Write:
		rlb.writer = node
	default:
		panic("Node has unknown or invalid mode.")
	}
}

func (rlb *MultiReaderSingleWriterBalancer) Acquire(ctx *Context) (*sql.DB, error) {
	mode := ctx.Mode
	switch mode {
	case Read:
		i := rand.Int31n(int32(len(rlb.readers)))
		return rlb.readers[i].db()
	case Write:
		return rlb.writer.db()
	default:
		return nil, errors.New(fmt.Sprintf("Unknown value for context key 'mode', expected 'read' or 'write', was %d", mode))

	}
	return nil, nil
}

func (rlb *MultiReaderSingleWriterBalancer) SetCluster(cluster *Cluster) {}

func (rlb *ParsingMultiReaderSingleWriterBalancer) Parse(sql string) int16 {
	sql_ := strings.TrimSpace(sql)
	sql_ = strings.ToLower(sql_[0:6])
	switch sql_ {
	case "select":
		if strings.Contains(sql, "/* mode: write */") {
			return Write
		} else {
			return Read
		}
	case "update":
		return Write
	case "insert":
		return Write
	case "delete":
		return Write
	}
	return -1
}

func (rlb *ParsingMultiReaderSingleWriterBalancer) Acquire(ctx *Context) (*sql.DB, error) {
	ctx.Mode = rlb.Parse(ctx.Sql)
	return rlb.MultiReaderSingleWriterBalancer.Acquire(ctx)
}
