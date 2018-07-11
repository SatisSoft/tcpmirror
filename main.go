package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"math/rand"
	"sync"
	"time"
	"github.com/ashirko/go-metrics"
	"github.com/ashirko/go-metrics-graphite"
	"net"
)

const max = 10
const testlen = 100
const vallen = 200

func main() {
	//test_latency()
	test_throughput()
}

func test_latency() {
	fmt.Println("start test latency")
	sample := metrics.NewUniformSample(10000)
	histogram := metrics.NewHistogram(sample)
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
		go func(num int, randval int64, total metrics.Counter, histogram metrics.Histogram) {
			defer wg.Done()
			time.Sleep(time.Duration(randval) * time.Millisecond)
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				fmt.Printf("error in %d connection: %s\n", num, err)
				return
			}
			defer c.Close()
			ch := make(chan int64, 60)
			go func(num int, ch chan int64){
				var score int64
				score =  <- ch
				res, err := redis.ByteSlices(c.Do("ZRANGEBYSCORE", num, score, score))
				if err != nil {
					fmt.Printf("error in ZRANGE for %d key: %s\n", num, err)
					return
				}
				latency := time.Now().UnixNano() - score
				fmt.Printf("get result of type %[1]T: %[1]v\n", res)
				fmt.Printf("latency: %d\n", latency)


			}(num, ch)
			for i := 0; i < testlen; i++ {
				nano := time.Now().UnixNano()
				val := make([]byte, vallen)
				rand.Read(val)
				_, err = c.Do("ZADD", num, nano, val)
				if err != nil {
					fmt.Printf("error in request for %d connection: %s\n", num, err)
					return
				}
				total.Inc(1)
				time.Sleep(1 * time.Second)
			}
		}(num, randval, total, histogram)
	}
	wg.Wait()
}


func test_throughput() {
	fmt.Println("start test throughput")
	counter := metrics.NewCustomCounter()
	metrics.Register("throughput", counter)
	counter1 := metrics.NewCustomCounter()
	metrics.Register("throughput", counter1)
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
		go func(num int, randval int64, total, counter1 metrics.Counter) {
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
				counter1.Inc(1)
				time.Sleep(1 * time.Second)
			}
		}(num, randval, total, counter1)
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

func getMill() int64 {
	return time.Now().UnixNano() / 1000000
}
