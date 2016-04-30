/*
Package go-loadbalancer is a simple Go library that adds load balancing capabilities on top of database/sql. It was built with lib/pq, the Postgres driver, in mind but should in theory work with other databases that do not already provide clustering/load balancing.

Example

    package main

    import (
        "fmt"
        "github.com/ericmort/go-loadbalancer"
        _ "github.com/lib/pq"
    )

    func main() {
        fmt.Println("Will run select ...")
        c := loadbalancer.Cluster{}
        node1 := &loadbalancer.Node{Url: "postgres://eric:eric@localhost/mydb?sslmode=disable", Type: "postgres"}
        node2 := &loadbalancer.Node{Url: "postgres://eric:eric@localhost/mydb?sslmode=disable", Type: "postgres"}

        // add nodes to cluster
        c.Add(node1)
        c.Add(node2)

        // acquire a dateabase connection to one of the nodes, using the default (random) load balancer
        conn, err := c.Acquire(nil)

        var result interface{}
        err = conn.QueryRow("select 1").Scan(&result)
        fmt.Printf("Result: %d %s\n", result, err)
    }

Load balancing strategis

There are several load balancing strategies you can use:

* Random load balancing (default)
* Multiple readers, single writer  (e.g., for Postgres Hot Standby setup)
* Multiple readers, single writer - with parsing (uses hints in SQL to determine if read or write node)

Random load balancing

The random load balancer randomly selects a node each time. On average this will distribute the load evenly. This is the default, and no special setup is required to use.

### Multiple readers, single writer

Many (most?) Postgres production environments use a Hot Standby architecture, where there exists a single writable master, and one or more readable standby servers. To use such a load balancer requires that the call to `Acquire()`provide a context that indicates whether you need a readable transaction or a writable transaction.

If you intend to execute SQL that only reads the database (e.g., `select * from dummy`)  then you can

    c := Cluster{}
    c.Acquire(&Context{mode: Read})

If you intend to execute SQL that only reads the database (e.g., `insert into dummy (x) values (1)`)  then you can


    c := Cluster{}
    c.Acquire(&Context{mode: Write})

### Multiple readers, single writer, with parsing

At Appstax we use stored functions a lot, which on Postgres are called from the client using `select myfunc(...)`. Because it is difficult to distinguish such a call from a regular select `select * from mytable`, we added a hint that we add to every Postgres function call that requires a writable connection.

For example:

    sql := "select /* mode: write */ myfunc_that_will_perform_updates()"
    c := Cluster{}
    c.Acquire(&Context{Sql: sql})

Now the call to `Acquire()` will parse the statement, looking for a `/* mode: write */` hint. If it finds it, the load balancer will open a connection to the `master` and not one of the `hot standbys`


### Add custom load balancers

...
*/
package go-loadbalancer
