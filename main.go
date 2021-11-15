package main

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"sync"
	"time"
)

const lua = `
	if redis.call("GET rds:lock") == ARGV[1] then
		return redis.call("del rds:lock")
	else 
		return 0
	end
`

type Pool struct {
	lock          sync.Mutex
	Max           int
	Conn          []redis.Conn
	UsingCoonFrom []time.Time
	Timeout       time.Duration
}

func NewPool(size int, timeout time.Duration) (p *Pool) {
	p = &Pool{
		Timeout:       timeout,
		Max:           size,
		Conn:          make([]redis.Conn, 0, 5),
		UsingCoonFrom: make([]time.Time, 0, 5),
	}
	return p
}

func (p *Pool) loop() {
	p.lock.Lock()
	defer p.lock.Unlock()
	unended := []time.Time{}
	for _, c := range p.UsingCoonFrom {
		if time.Now().Sub(c) <= p.Timeout {
			unended = append(unended, c)
		}
	}
	p.UsingCoonFrom = unended
	lenn := p.Max - len(p.UsingCoonFrom) - len(p.Conn)
	for i := 1; i <= lenn; i++ {
		conn, err := redis.Dial("tcp", "127.0.0.1:6379")
		if err != nil {
			return
		}
		p.Conn = append(p.Conn, conn)
	}
	return
}

func (p *Pool) Get() (conn redis.Conn) {
	p.loop()
	// more than cap
	if len(p.Conn) <= 0 {
		return nil
	}
	conn = p.Conn[0]
	p.Conn = p.Conn[1:]
	p.UsingCoonFrom = append(p.UsingCoonFrom, time.Now().UTC())
	return
}

// 假设业务最长时间是10s
func Lock(conn redis.Conn, uuid int64) (err error) {
	res, err := redis.String(conn.Do("SET", "rds:lock", uuid, "EX", 30, "NX"))
	if err == redis.ErrNil {
		err = errors.New("锁占用")
	}
	if res == "" {
		err = errors.New("锁占用")
	}
	if res != "OK" {
		err = errors.New("FAIL")
	}
	return
}

func Unlock(conn redis.Conn, uuid int64) (err error) {
	id, err := redis.Int64(redis.Int64(conn.Do("GET", "rds:lock")))
	if err != nil {
		return err
	}
	if id != uuid {
		return errors.New("NOT OWNER LOCK")
	}
	reply, err := redis.Int(conn.Do("DEL", "rds:lock"))
	if reply != 1 && err == nil {
		err = errors.New("DEL ERROR")
	}
	return err
}

func main() {
	p := NewPool(5, time.Second*20)
	wg := sync.WaitGroup{}
	for i := 1; i <= 100; i++ {
		wg.Add(1)
		go func(uuid int64) {
			defer wg.Done()
			conn := p.Get()
			if conn == nil {
				return
			}
			err := Lock(conn, uuid)
			if err != nil {
				return
			}
			defer func() {
				err = Unlock(conn, uuid)
				fmt.Println(err)
				if err != nil {
					return
				}
			}()
			// do something
			return
		}(int64(i))
	}
	wg.Wait()
}
