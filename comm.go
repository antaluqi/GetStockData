// comm
package main

import (
	"fmt"
	//"math"
	"strconv"
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

// 日期加减
func dayAdd(day string, ad int) string {
	t, _ := time.Parse("2006-01-02", day)
	adh, _ := time.ParseDuration(fmt.Sprintf("%dh", 24*ad))
	rt := t.Add(adh)
	return rt.Format("2006-01-02")
}

//日期取小
func dayMin(day1, day2 string) string {
	t1, _ := time.Parse("2006-01-02", day1)
	t2, _ := time.Parse("2006-01-02", day2)
	if t1.Before(t2) {
		return day1
	} else {
		return day2
	}
}

//按照指定间隔分割时间段
func daySplit(endD, startD string, sNo int) [][]string {
	var result [][]string
	No := daySub(endD, startD)
	st := startD
	ed := endD
	for i := 0; i < No; {
		ed = dayMin(endD, dayAdd(st, sNo))
		i = i + sNo
		iresult := []string{st, ed, strconv.Itoa(daySub(ed, st))}
		result = append(result, iresult)

		st = dayAdd(ed, 1)
	}
	return result
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

// 字符串矩阵倒置
func strArrRev(arrIn []string) []string {
	L := len(arrIn)
	var result []string
	for i, _ := range arrIn {
		result = append(result, arrIn[L-i-1])
	}
	return result
}
