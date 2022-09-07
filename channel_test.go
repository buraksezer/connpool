package connpool

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	InitialCap = 5
	MaximumCap = 30
	network    = "tcp"
	address    = "127.0.0.1:7777"
	factory    = func() (net.Conn, error) { return net.Dial(network, address) }
)

func init() {
	// used for factory function
	go simpleTCPServer()
	time.Sleep(time.Millisecond * 300) // wait until tcp server has been settled

	rand.Seed(time.Now().UTC().UnixNano())
}

func TestNew(t *testing.T) {
	_, err := newChannelPool()
	if err != nil {
		t.Errorf("New error: %s", err)
	}
}

func TestPool_Get_Impl(t *testing.T) {
	p, _ := newChannelPool()
	defer p.Close()

	conn, err := p.Get(context.Background())
	if err != nil {
		t.Errorf("Get error: %s", err)
	}

	_, ok := conn.(*PoolConn)
	if !ok {
		t.Errorf("Conn is not of type poolConn")
	}
}

func TestPool_Get(t *testing.T) {
	p, _ := newChannelPool()
	defer p.Close()

	_, err := p.Get(context.Background())
	if err != nil {
		t.Errorf("Get error: %s", err)
	}

	// after one get, current capacity should be lowered by one.
	if p.Len() != (InitialCap - 1) {
		t.Errorf("Get error. Expecting %d, got %d",
			(InitialCap - 1), p.Len())
	}

	// get them all
	var wg sync.WaitGroup
	for i := 0; i < (InitialCap - 1); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.Get(context.Background())
			if err != nil {
				t.Errorf("Get error: %s", err)
			}
		}()
	}
	wg.Wait()

	if p.Len() != 0 {
		t.Errorf("Get error. Expecting %d, got %d",
			InitialCap-1, p.Len())
	}

	_, err = p.Get(context.Background())
	if err != nil {
		t.Errorf("Get error: %s", err)
	}
}

func TestPool_Put(t *testing.T) {
	p, err := NewChannelPool(0, 30, factory)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// get/create from the pool
	conns := make([]net.Conn, MaximumCap)
	for i := 0; i < MaximumCap; i++ {
		conn, _ := p.Get(context.Background())
		conns[i] = conn
	}

	// now put them all back
	for _, conn := range conns {
		conn.Close()
	}

	if p.Len() != MaximumCap {
		t.Errorf("Put error len. Expecting %d, got %d",
			1, p.Len())
	}

	conn, _ := p.Get(context.Background())
	p.Close() // close pool

	conn.Close() // try to put into a full pool
	if p.Len() != 0 {
		t.Errorf("Put error. Closed pool shouldn't allow to put connections.")
	}
}

func TestPool_PutUnusableConn(t *testing.T) {
	p, _ := newChannelPool()
	defer p.Close()

	// ensure pool is not empty
	conn, _ := p.Get(context.Background())
	conn.Close()

	poolSize := p.Len()
	conn, _ = p.Get(context.Background())
	conn.Close()
	if p.Len() != poolSize {
		t.Errorf("Pool size is expected to be equal to initial size")
	}

	conn, _ = p.Get(context.Background())
	if pc, ok := conn.(*PoolConn); !ok {
		t.Errorf("impossible")
	} else {
		pc.MarkUnusable()
	}
	conn.Close()
	if p.Len() != poolSize-1 {
		t.Errorf("Pool size is expected to be initial_size - 1 (%d), got:%d", poolSize-1, p.Len())
	}
}

func TestPool_UsedCapacity(t *testing.T) {
	p, _ := newChannelPool()
	defer p.Close()

	if p.Len() != InitialCap {
		t.Errorf("InitialCap error. Expecting %d, got %d",
			InitialCap, p.Len())
	}
}

func TestPool_Close(t *testing.T) {
	p, _ := newChannelPool()

	// now close it and test all cases we are expecting.
	p.Close()

	c := p.(*channelPool)

	if c.conns != nil {
		t.Errorf("Close error, conns channel should be nil")
	}

	if c.factory != nil {
		t.Errorf("Close error, factory should be nil")
	}

	_, err := p.Get(context.Background())
	if err == nil {
		t.Errorf("Close error, get conn should return an error")
	}

	if p.Len() != 0 {
		t.Errorf("Close error used capacity. Expecting 0, got %d", p.Len())
	}
}

func TestPoolConcurrent(t *testing.T) {
	p, _ := newChannelPool()
	pipe := make(chan net.Conn)

	go func() {
		p.Close()
	}()

	for i := 0; i < MaximumCap; i++ {
		go func() {
			conn, _ := p.Get(context.Background())

			pipe <- conn
		}()

		go func() {
			conn := <-pipe
			if conn == nil {
				return
			}
			conn.Close()
		}()
	}
}

func TestPoolWriteRead(t *testing.T) {
	p, _ := NewChannelPool(0, 30, factory)

	conn, _ := p.Get(context.Background())

	msg := "hello"
	_, err := conn.Write([]byte(msg))
	if err != nil {
		t.Error(err)
	}
}

func TestPoolConcurrent2(t *testing.T) {
	p, _ := NewChannelPool(0, 30, factory)

	var wg sync.WaitGroup

	go func() {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				conn, _ := p.Get(context.Background())
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
				conn.Close()
				wg.Done()
			}(i)
		}
	}()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			conn, _ := p.Get(context.Background())
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
			conn.Close()
			wg.Done()
		}(i)
	}

	wg.Wait()
}

func TestPoolConcurrent3(t *testing.T) {
	p, _ := NewChannelPool(0, 1, factory)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		p.Close()
		wg.Done()
	}()

	if conn, err := p.Get(context.Background()); err == nil {
		conn.Close()
	}

	wg.Wait()
}

func newChannelPool() (Pool, error) {
	return NewChannelPool(InitialCap, MaximumCap, factory)
}

func simpleTCPServer() {
	l, err := net.Listen(network, address)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			panic(err)
		}

		go func() {
			buffer := make([]byte, 256)
			conn.Read(buffer)
		}()
	}
}

func TestPoolMaximumCapacity(t *testing.T) {
	var successful int32
	p, _ := NewChannelPool(InitialCap, MaximumCap, func() (net.Conn, error) {
		return net.Dial(network, address)
	})
	defer p.Close()

	var wg sync.WaitGroup
	numWorkers := MaximumCap * 2
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
			defer cancel()

			_, err := p.Get(ctx)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return
				}
				t.Errorf("expected nil, got: %v", err)
				return
			}
			atomic.AddInt32(&successful, 1)
		}()
	}

	wg.Wait()

	if atomic.LoadInt32(&successful) != int32(MaximumCap) {
		t.Errorf("expected successfull Get calls: %d, got: %d", MaximumCap, successful)
	}

	if p.NumberOfConns() != MaximumCap {
		t.Errorf("expected connection count %d, got %d", MaximumCap, p.NumberOfConns())
	}
}

func TestPoolMaximumCapacity_Close(t *testing.T) {
	var successful int32
	p, _ := NewChannelPool(InitialCap, MaximumCap, func() (net.Conn, error) {
		return net.Dial(network, address)
	})
	defer p.Close()

	var wg sync.WaitGroup
	numWorkers := MaximumCap * 2

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
			defer cancel()

			c, err := p.Get(ctx)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return
				}
				t.Errorf("expected nil, got: %v", err)
				return
			}

			atomic.AddInt32(&successful, 1)
			c.Close()
		}()
	}

	wg.Wait()

	if atomic.LoadInt32(&successful) != int32(numWorkers) {
		t.Errorf("expected successful Get calls: %d, got: %d",
			numWorkers, atomic.LoadInt32(&successful))
	}
}

func TestPool_Get1(t *testing.T) {
	p, _ := NewChannelPool(0, 1, factory)
	defer p.Close()

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, _ := p.Get(context.Background())
			defer conn.Close()

			time.Sleep(time.Millisecond * 100)
		}()
	}
	wg.Wait()
}

func TestPool_ClosedConnectionsReplaced(t *testing.T) {
	p, _ := NewChannelPool(1, 1, factory)
	defer p.Close()

	conn, _ := p.Get(context.Background())

	conn.(*PoolConn).MarkUnusable()
	conn.Close()

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Millisecond)
	conn, err := p.Get(ctx)
	if err != nil {
		t.Errorf("expected nil, got: %v", err)
	}
	if conn == nil {
		t.Errorf("expected non-nil conn")
	}
}

func TestPool_FailedFactoryRestoresConnectionSlot(t *testing.T) {
	// regression test to ensure failed factory doesn't permanently decrease connection max
	// this happened previously due to semaphore not being released when factory failed
	factory := func() (net.Conn, error) {
		// a dial guaranteed to fail
		return net.DialTimeout("tcp", "localhost:1234", time.Millisecond)
	}

	max := 2
	pool, err := NewChannelPool(0, max, factory)
	if err != nil {
		t.Fatalf("error creating pool: %v", err)
	}

	for i := 0; i < max+1; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()

		conn, err := pool.Get(ctx)
		if err == nil && conn != nil {
			conn.Close()
		}
	}

	if len(pool.(*channelPool).semaphore) != 0 {
		t.Fatal("max connections decreased")
	}
}
