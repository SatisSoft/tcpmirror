package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strconv"
	"sync"
	"time"
)

const max = 10

func main() {
	fmt.Println("start execution")
	var wg sync.WaitGroup
	for num := 0; num < max; num++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			fmt.Println("start execution goroutine")
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				fmt.Printf("error in %d connection: %s\n", num, err)
				return
			}
			defer c.Close()
			fmt.Printf("%d connection established\n", num)
			for {
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
	wg.Wait()
}

func getMill() int64 {
	return time.Now().UnixNano() / 100000
}
