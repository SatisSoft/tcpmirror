package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strconv"
	"time"
	"sync"
)

func main() {
	fmt.Println("start execution")
	var wg sync.WaitGroup
	for num := 0; num < 1; num++ {
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
			millisecs := getMill()
			for i := 0; i < 10; i++ {
				val := strconv.Itoa(num) + ":" + strconv.FormatInt(millisecs, 10)
				_, err = c.Do("ZADD", num, millisecs, val)
				if err != nil  {
					fmt.Printf("error in request for %d connection: %s\n", num, err)
					return
				}

			}
			res1, err := c.Do("ZRANGE", num, 0, 0)
			if err != nil  {
				fmt.Printf("error in request for %d connection: %s\n", num, err)
				return
			}
			fmt.Printf("%d connection result 1: %v\n", num, res1)
			res2, err := c.Do("ZRANGE", num, 0, 2)
			if err != nil  {
				fmt.Printf("error in request for %d connection: %s\n", num, err)
				return
			}
			fmt.Printf("%d connection result 2: %v\n", num, res2)
			res3, err := c.Do("ZRANGEBYSCORE", num, 0, getMill())
			if err != nil  {
				fmt.Printf("error in request for %d connection: %s\n", num, err)
				return
			}
			fmt.Printf("%d connection result 3: %v\n", num, res3)
			res4, err := c.Do("ZRANGEBYSCORE", num, 0, getMill())
			if err != nil  {
				fmt.Printf("error in request for %d connection: %s\n", num, err)
				return
			}
			fmt.Printf("%d connection result 4: %v\n", num, res4)
		}(num)
	}
	wg.Wait()
}

func getMill() int64 {
	return time.Now().UnixNano() / 100000
}
