package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strconv"
	"sync"
	"time"
)

const max = 1

func main() {
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
			for i := 0; i <10; i++ {
				millisecs := getMill()
				val := strconv.FormatInt(time.Now().UnixNano(), 10) + ":" + strconv.Itoa(i)
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
			time.Sleep(5)
			//res, err := c.Do("ZRANGE", num, 0, 0)
			res, err := redis.Int64(c.Do("ZRANGE", num, 0, 0))
			if err != nil {
				fmt.Printf("error in ZRANGE for %d key: %s\n", num, err)
				return
			}
			fmt.Printf("result of type %[1]T: %[1]v\n", res),

		}(num)
	}
	wg.Wait()
}

func getMill() int64 {
	return time.Now().UnixNano() / 100000
}
