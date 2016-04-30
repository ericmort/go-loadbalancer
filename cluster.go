package loadbalancer

import (
	"database/sql"
	"regexp"
)

type Cluster struct {
	Nodes              []*Node
	lb                 LoadBalancer
	MaxIdleConnections int
}

type Node struct {
	Url     string
	RegEx   regexp.Regexp
	db_     *sql.DB
	cluster *Cluster
	Type    string
	values  map[string]string
	Mode    int16
}

type Context struct {
	Mode   int16
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

func (c *Cluster) SetLoadBalancer(lb LoadBalancer) {
	c.lb = lb
	lb.SetCluster(c)
}

func (c *Cluster) Acquire(context *Context) (*sql.DB, error) {
	return c.lb.Acquire(context)
}
