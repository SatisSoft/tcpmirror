package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"math/rand"
	"strconv"
	"sync"
	"time"
	"github.com/ashirko/go-metrics"
	"github.com/ashirko/go-metrics-graphite"
	"net"
)

const max = 10000
const testlen = 3600
const vallen = 200

func main() {
	test_throughput()
}

func test_throughput() {
	fmt.Println("start test throughput")
	counter := metrics.NewCustomCounter()
	metrics.Register("throughput", counter)
	total := metrics.NewCounter()
	metrics.Register("total", total)
	addr, err := net.ResolveTCPAddr("tcp", "10.1.116.51:2003")
	if err != nil{
		fmt.Printf("error while connection to graphite: %s\n", err)
	}
	go graphite.Graphite(metrics.DefaultRegistry, 10e9, "tcpmirror.metrics", addr)
	var wg sync.WaitGroup
	for num := 0; num < max; num++ {
		randval := rand.Int63n(10000)
		wg.Add(1)
		go func(num int, randval int64, total metrics.Counter) {
			defer wg.Done()
			time.Sleep(time.Duration(randval) * time.Millisecond)
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				fmt.Printf("error in %d connection: %s\n", num, err)
				return
			}
			defer c.Close()
			for i := 0; i < testlen; i++ {
				millisecs := getMill()
				val := make([]byte, vallen)
				rand.Read(val)
				_, err = c.Do("ZADD", num, millisecs, val)
				if err != nil {
					fmt.Printf("error in request for %d connection: %s\n", num, err)
					return
				}
				total.Inc(1)
				time.Sleep(1 * time.Second)
			}
		}(num, randval, total)
	}
	for num := 0; num < max; num++ {
		wg.Add(1)
		go func(num int, counter metrics.Counter) {
			defer wg.Done()
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				fmt.Printf("error in %d connection: %s\n", num, err)
				return
			}
			defer c.Close()
			time.Sleep(1 * time.Second)
			for i := 0; i < testlen; i++ {
				to := getMill()
				from := to - 1000
				res, err := redis.ByteSlices(c.Do("ZRANGEBYSCORE", num, from, to))
				if err != nil {
					fmt.Printf("error in ZRANGE for %d key: %s\n", num, err)
					return
				}
				time.Sleep(1 * time.Second)
				counter.Inc(int64(len(res)))
			}

		}(num, counter)
	}
	wg.Wait()
}

func test1() {
	fmt.Println("start execution")
	var wg sync.WaitGroup
	for num := 0; num < max; num++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			//fmt.Println("start execution goroutine")
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				fmt.Printf("error in %d connection: %s\n", num, err)
				return
			}
			defer c.Close()
			for i := 0; i < 10; i++ {
				millisecs := getMill()
				val := strconv.FormatInt(time.Now().UnixNano(), 10)
				_, err = c.Do("ZADD", num, millisecs, val)
				if err != nil {
					fmt.Printf("error in request for %d connection: %s\n", num, err)
					return
				}
				time.Sleep(1 * time.Second)
			}
		}(num)
	}
	for num := 0; num < max; num++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				fmt.Printf("error in %d connection: %s\n", num, err)
				return
			}
			defer c.Close()
			time.Sleep(5 * time.Second)
			res, err := redis.Int64s(c.Do("ZRANGE", num, 0, 0))
			if err != nil {
				fmt.Printf("error in ZRANGE for %d key: %s\n", num, err)
				return
			}
			fmt.Printf("result of type %[1]T: %[1]v\n", res)
			fmt.Printf("res[0] of type %[1]T: %[1]v\n", res[0])

		}(num)
	}
	wg.Wait()
}

func getMill() int64 {
	return time.Now().UnixNano() / 1000000
}
