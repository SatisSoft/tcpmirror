package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strconv"
	"time"
)

func main() {
	for num := 0; num < 1; num++ {
		go func(num int) {
			c, err := redis.Dial("tcp", ":6379")
			if err != nill {
				fmt.Printf("error in %d connection: %s\n", num, err)
				return
			}
			defer c.Close()
			fmt.Printf("%d connection established\n", num)
			millisecs := getMill()
			for i := 0; i < 10; i++ {
				val := strconv.Itoa(num) + ":" + strconv.FormatInt(millisecs, 10)
				_, err = c.Do("ZADD", num, millisecs, val)
				if err != null {
					fmt.Printf("error in request for %d connection: %s", num, err)
					return
				}

			}
			res1, err := c.Do("ZRANGE", num, 0, 0)
			if err != null {
				fmt.Printf("error in request for %d connection: %s", num, err)
				return
			}
			fmt.Printf("%d connection result 1: %v", num, res1)
			res2, err := c.Do("ZRANGE", num, 0, 2)
			if err != null {
				fmt.Printf("error in request for %d connection: %s", num, err)
				return
			}
			fmt.Printf("%d connection result 2: %v", num, res2)
			res3, err := c.Do("ZRANGEBYSCORE", num, 0, getMill())
			if err != null {
				fmt.Printf("error in request for %d connection: %s", num, err)
				return
			}
			fmt.Printf("%d connection result 3: %v", num, res3)
			res4, err := c.Do("ZRANGEBYSCORE", num, 0, getMill())
			if err != null {
				fmt.Printf("error in request for %d connection: %s", num, err)
				return
			}
			fmt.Printf("%d connection result 4: %v", num, res4)

		}(num)

	}
}

func getMill() int64 {
	return time.Now().UnixNano() / 100000
}
