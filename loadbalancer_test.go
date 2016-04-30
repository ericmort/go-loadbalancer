package loadbalancer

import (
	"database/sql"
	"strconv"
	"testing"
)

const (
	TEST_NUMBER_OF_CONNS = 100000
	TEST_NUMBER_OF_NODES = 100
	MIN_PERCENT          = 0.8
	MAX_PERCENT          = 1.2
)

func TestAcquire_Random(t *testing.T) {
	c := Cluster{}

	for i := 0; i < TEST_NUMBER_OF_NODES; i++ {
		c.Add(&Node{Url: strconv.Itoa(i), Type: "dummy"})
	}

	dbconns := make(map[*sql.DB]int)
	conns := make([]*sql.DB, TEST_NUMBER_OF_CONNS)
	for i := 0; i < TEST_NUMBER_OF_CONNS; i++ {
		conns[i], _ = c.Acquire(nil)
		dbconns[conns[i]] += 1
		conns[i].Prepare("dummy query")
	}

	for _, cnt := range dbconns {
		if cnt < TEST_NUMBER_OF_CONNS/TEST_NUMBER_OF_NODES*MIN_PERCENT || cnt > TEST_NUMBER_OF_CONNS/TEST_NUMBER_OF_NODES*MAX_PERCENT {
			t.Errorf("RandomLoadBalancer.Acquire algorithm did not distribute nodes sufficiently randomly")
			return
		}

	}
}

func TestAdd_MultiReadSingleWrite(t *testing.T) {
	c := Cluster{}

	c.SetLoadBalancer(&MultiReaderSingleWriterBalancer{})
	for i := 0; i < 10; i++ {
		c.Add(&Node{Url: "1", Type: "dummy", Mode: Read})
	}
	c.Add(&Node{Url: "2", Type: "dummy", Mode: Write})

	lenReaders := len(c.lb.(*MultiReaderSingleWriterBalancer).readers)
	if lenReaders != 10 {
		t.Errorf("Expected number of readers to be 10, was %d", lenReaders)
	}
}

func TestAcquire_MultiReadSingleWrite(t *testing.T) {
	c := Cluster{}

	c.SetLoadBalancer(&MultiReaderSingleWriterBalancer{})
	for i := 0; i < TEST_NUMBER_OF_NODES; i++ {
		c.Add(&Node{Url: strconv.Itoa(i), Type: "dummy", Mode: Read})
	}
	c.Add(&Node{Url: "1", Type: "dummy", Mode: Write})

	readerconns := make(map[*sql.DB]int)
	conns := make([]*sql.DB, TEST_NUMBER_OF_CONNS)
	var readContext = &Context{
		Mode: Read,
	}
	var writeContext = &Context{
		Mode: Write,
	}

	for i := 0; i < TEST_NUMBER_OF_CONNS; i++ {
		conns[i], _ = c.Acquire(readContext)
		readerconns[conns[i]] += 1
		conns[i].Prepare("dummy query")
	}

	writeConn, _ := c.Acquire(writeContext)
	writeConn.Prepare("dummy write query")

	for _, cnt := range readerconns {
		if cnt < TEST_NUMBER_OF_CONNS/TEST_NUMBER_OF_NODES*MIN_PERCENT || cnt > TEST_NUMBER_OF_CONNS/TEST_NUMBER_OF_NODES*MAX_PERCENT {
			t.Errorf("MultiReaderSingleWriterBalancer.Acquire algorithm did not distribute reader nodes sufficiently randomly (counts between 90%% and 110%% of expected value: cnt=%d", cnt)
			return
		}

	}
}

func TestAcquire_MultiReadSingleWrite_WithParsing(t *testing.T) {
	c := Cluster{}

	c.SetLoadBalancer(&ParsingMultiReaderSingleWriterBalancer{})
	for i := 0; i < TEST_NUMBER_OF_NODES; i++ {
		c.Add(&Node{Url: strconv.Itoa(i), Type: "dummy", Mode: Read})
	}
	c.Add(&Node{Url: "1", Type: "dummy", Mode: Write})

	readerconns := make(map[*sql.DB]int)
	conns := make([]*sql.DB, TEST_NUMBER_OF_CONNS)
	var readContext = &Context{
		Sql: "select /* mode: read */ * from dummy",
	}
	var writeContext = &Context{
		Sql: "select /* mode: write */ dummy2()",
	}
	for i := 0; i < TEST_NUMBER_OF_CONNS; i++ {
		conns[i], _ = c.Acquire(readContext)
		readerconns[conns[i]] += 1
		conns[i].Prepare("dummy query")
	}

	writeConn, _ := c.Acquire(writeContext)
	writeConn.Prepare("dummy write query")

	sum := 0
	for _, cnt := range readerconns {
		sum += cnt
		if cnt < TEST_NUMBER_OF_CONNS/TEST_NUMBER_OF_NODES*MIN_PERCENT || cnt > TEST_NUMBER_OF_CONNS/TEST_NUMBER_OF_NODES*MAX_PERCENT {
			t.Errorf("ParsingMultiReaderSingleWriterBalancer.Acquire algorithm did not distribute reader nodes sufficiently randomly (counts between 90%% and 110%% of expected value: cnt=%d", cnt)
			return
		}
	}
	if sum != TEST_NUMBER_OF_CONNS {
		t.Errorf("ParsingMultiReaderSingleWriterBalancer.Acquire did not generate %d connections, got %d instead", sum, TEST_NUMBER_OF_CONNS)
	}
}
