// comm
package main

import (
	"fmt"
	"time"
)

// 整形最小值（两个数）
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// 整形最大值（两个数）
func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// 打印错误
func check(err error) {

	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}

// 日期差
func daySub(endD, startD string) int {
	t1, _ := time.Parse("2006-01-02", endD)
	t2, _ := time.Parse("2006-01-02", startD)
	t1 = t1.UTC().Truncate(24 * time.Hour)
	t2 = t2.UTC().Truncate(24 * time.Hour)
	return int(t1.Sub(t2).Hours() / 24)
}

// []string以splitStr为间隔串联为一个string

func array2str(array []string, splitStr string) string {
	result := ""
	for _, v := range array {
		result = result + v + splitStr
	}
	if len(result) > 0 {
		result = result[0 : len(result)-len(splitStr)]
	}
	return result
}
