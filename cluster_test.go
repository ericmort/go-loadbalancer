// Copyright 2016 Eric Mortensen. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package loadbalancer

import (
	"database/sql"
	"database/sql/driver"
	"testing"
)

func hasNode(cluster Cluster, node *Node) bool {
	for _, v := range cluster.Nodes {
		if v == node {
			return true
		}
	}
	return false
}

func TestAddNodes_DefaultLoadBalancer(t *testing.T) {
	c := Cluster{}
	node1 := &Node{Url: "postgres://10.2.6.100:5432/appstax?sslmode=disable", Type: "postgres"}
	node2 := &Node{Url: "postgres://10.2.5.100:5432/appstax?sslmode=disable", Type: "postgres"}

	// basic test
	c.Add(node1)
	c.Add(node2)

	if !hasNode(c, node1) {
		t.Errorf("node1 not found in cluster")
	}
	if !hasNode(c, node2) {
		t.Errorf("node2 not found in cluster")
	}

	_, ok := c.lb.(*RandomLoadBalancer)
	if !ok {
		t.Errorf("Default load balancer not RandomLoadBalancer")
	}
	//
}

type DummyLoadBalancer struct {
	LoadBalancer
	dummy string
	node  *Node
}

func (d *DummyLoadBalancer) Add(node *Node) {
	d.dummy = "Add was called"
	d.node = node
}

func (d *DummyLoadBalancer) Acquire(ctx *Context) (*sql.DB, error) {
	d.dummy = "Acquire was called"
	return d.node.db()
}

func (d *DummyLoadBalancer) SetCluster(c *Cluster) {

}

func TestAddNodes_NonDefaultLB(t *testing.T) {
	c := Cluster{}
	node1 := &Node{Url: "postgres://10.2.6.100:5432/appstax?sslmode=disable"}
	node2 := &Node{Url: "postgres://10.2.5.100:5432/appstax?sslmode=disable"}

	d := &DummyLoadBalancer{}
	c.SetLoadBalancer(d)
	c.Add(node1)
	c.Add(node2)

	if !hasNode(c, node1) {
		t.Errorf("node1 not found in cluster")
	}
	if d.dummy != "Add was called" {
		t.Errorf("Add was not called on DummyLoadBalancer for node 1")
	}
	if !hasNode(c, node2) {
		t.Errorf("node2 not found in cluster")
	}
	if d.dummy != "Add was called" {
		t.Errorf("Add was not called on DummyLoadBalancer for node 2")
	}

	_, ok := c.lb.(*DummyLoadBalancer)
	if !ok {
		t.Errorf("Load balancer not DummyLoadBalancer")
	}
	//
}

type drv struct {
	dummy string
}

func (d *drv) Open(name string) (driver.Conn, error) {
	d.dummy = name
	return conn{}, nil
}

var dummyDriver = drv{dummy: "initial"}

func init() {
	sql.Register("dummy", &dummyDriver)
}

type conn struct{}

func (c conn) Prepare(query string) (driver.Stmt, error) { return nil, nil }
func (c conn) Begin() (driver.Tx, error)                 { return nil, nil }
func (c conn) Close() error                              { return nil }

func TestAcquire_NonDefaultLB(t *testing.T) {
	c := Cluster{}
	node1 := &Node{Url: "1", Type: "dummy"}

	d := &DummyLoadBalancer{}
	c.SetLoadBalancer(d)
	c.Add(node1)

	ctx := &Context{}
	conn, _ := c.Acquire(ctx)
	if d.dummy != "Acquire was called" {
		t.Errorf("Acquire was not called on DummyLoadBalancer for node 1")
	}
	conn.Prepare("dummy query")
	if dummyDriver.dummy != "1" {
		t.Errorf("sql.Open() was not called for Acquire")
	}

}

func TestAcquire_MaxIdleConnections(t *testing.T) {
	c := Cluster{MaxIdleConnections: 20}
	node1 := &Node{Url: "1", Type: "dummy"}

	d := &DummyLoadBalancer{}
	c.SetLoadBalancer(d)
	c.Add(node1)

	ctx := &Context{}
	conn, _ := c.Acquire(ctx)
	if d.dummy != "Acquire was called" {
		t.Errorf("Acquire was not called on DummyLoadBalancer for node 1")
	}
	conn.Prepare("dummy query")
	if dummyDriver.dummy != "1" {
		t.Errorf("sql.Open() was not called for Acquire")
	}

}
