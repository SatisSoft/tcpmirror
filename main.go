package main

import (
	"fmt"
	"github.com/ashirko/go-metrics"
	"github.com/ashirko/go-metrics-graphite"
	"github.com/gomodule/redigo/redis"
	"math/rand"
	"net"
	"sync"
	"time"
)

const max = 500
const testlen = 1800
const vallen = 200

func main() {
	testLatAndTh_rnis()
}

func testLatAndTh_rnis() {
	fmt.Println("start test latency (rnis)")
	gauge := metrics.NewGauge()
	counter := metrics.NewCustomCounter()
	total := metrics.NewCounter()
	metrics.Register("throughput", counter)
	metrics.Register("exec_time", gauge)
	metrics.Register("total", total)
	addr, err := net.ResolveTCPAddr("tcp", "10.1.116.51:2003")
	if err != nil {
		fmt.Printf("error while connection to graphite: %s\n", err)
	}
	go graphite.Graphite(metrics.DefaultRegistry, 5*10e8, "tcpmirror.metrics", addr)
	var wg sync.WaitGroup
	wg.Add(1)
	go periodicalLatencyCheck_rnis(&wg, gauge)
	for num := 0; num < max; num++ {
		randval := rand.Int63n(1000)
		wg.Add(1)
		go periodicalWrite_rnis(&wg, num, randval, counter, total)
	}
	wg.Wait()
}

func periodicalWrite_rnis(wg *sync.WaitGroup, num int, randval int64, counter, total metrics.Counter) {
	defer wg.Done()
	time.Sleep(time.Duration(randval) * time.Millisecond)
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Printf("error in %d connection: %s\n", num, err)
		return
	}
	defer c.Close()
	for i := 0; i < testlen; i++ {
		mil := getMill()
		val := make([]byte, vallen)
		rand.Read(val)
		_, err = c.Do("ZADD", "rnis", mil, val)
		if err != nil {
			fmt.Printf("error in request for %d connection: %s\n", num, err)
			return
		}
		counter.Inc(1)
		total.Inc(1)
		time.Sleep(1 * time.Second)
	}
}

func periodicalLatencyCheck_rnis(wg *sync.WaitGroup, gauge metrics.Gauge) {
	defer wg.Done()
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Printf("error in read connection: %s\n", err)
		return
	}
	defer c.Close()
	len1 := testlen/5 +1
	for i := 0; i < len1; i++ {
		time0 := getMill()
		_, err := redis.ByteSlices(c.Do("ZRANGEBYSCORE", "rnis", 0, 50))
		if err != nil {
			fmt.Printf("error in ZRANGE: %s\n", err)
			return
		}
		latency := getMill() - time0
		gauge.Update(latency)
		time.Sleep(5 * time.Second)
	}
}

func testLatAndTh_mgt() {
	fmt.Println("start test latency")
	sample := metrics.NewUniformSample(2500)
	histogram := metrics.NewHistogram(sample)
	counter := metrics.NewCustomCounter()
	total := metrics.NewCounter()
	metrics.Register("throughput", counter)
	metrics.Register("latency", histogram)
	metrics.Register("total", total)
	addr, err := net.ResolveTCPAddr("tcp", "10.1.116.51:2003")
	if err != nil {
		fmt.Printf("error while connection to graphite: %s\n", err)
	}
	go graphite.Graphite(metrics.DefaultRegistry, 5*10e8, "tcpmirror.metrics", addr)
	var wg sync.WaitGroup
	for num := 0; num < max; num++ {
		randval := rand.Int63n(1000)
		wg.Add(1)
		go periodicalWrite_mgt(&wg, num, randval, counter, total, histogram)
	}
	wg.Wait()
}

func periodicalWrite_mgt(wg *sync.WaitGroup, num int, randval int64, counter, total metrics.Counter, histogram metrics.Histogram) {
	defer wg.Done()
	time.Sleep(time.Duration(randval) * time.Millisecond)
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Printf("error in %d connection: %s\n", num, err)
		return
	}
	defer c.Close()
	ch := make(chan int64, 60)
	go periodicalLatencyCheck_mgt(num, ch, histogram)
	for i := 0; i < testlen; i++ {
		mil := getMill()
		val := make([]byte, vallen)
		rand.Read(val)
		_, err = c.Do("ZADD", num, mil, val)
		if err != nil {
			fmt.Printf("error in request for %d connection: %s\n", num, err)
			return
		}
		ch <- mil
		counter.Inc(1)
		total.Inc(1)
		time.Sleep(1 * time.Second)
	}
}

func periodicalLatencyCheck_mgt(num int, ch chan int64, histogram metrics.Histogram) {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Printf("error in %d connection: %s\n", num, err)
		return
	}
	defer c.Close()
	for i := 0; i < testlen; i++ {
		score := <-ch
		_, err := redis.ByteSlices(c.Do("ZRANGEBYSCORE", num, score, score))
		if err != nil {
			fmt.Printf("error in ZRANGE for %d key: %s\n", num, err)
			return
		}
		latency := getMill() - score
		histogram.Update(latency)
	}
}

//depricated, use function testLatAndTh
func test_throughput() {
	fmt.Println("start test throughput")
	counter := metrics.NewCustomCounter()
	metrics.Register("throughput", counter)
	counter1 := metrics.NewCustomCounter()
	metrics.Register("throughput1", counter1)
	total := metrics.NewCounter()
	metrics.Register("total", total)
	addr, err := net.ResolveTCPAddr("tcp", "10.1.116.51:2003")
	if err != nil {
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
