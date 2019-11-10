// Http_test project main.go
package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	//"github.com/json-iterator/go"
)

// 获取股票列表（已失效）
func stocklist2() []string {
	URL := "http://quote.eastmoney.com/stocklist.html" // 代码下载网址
	//---------------------------------下载
	resp, err := http.Get(URL)
	check(err)
	defer resp.Body.Close()
	buf := bytes.NewBuffer(make([]byte, 0, 512))
	buf.ReadFrom(resp.Body)
	// --------------------------------正则表达式取数据
	var htmlByte = buf.Bytes()
	reg, err := regexp.Compile(`<li><a target="_blank" href="http://quote.eastmoney.com/(\S\S.*?).html">`)
	sub := reg.FindAllStringSubmatch(string(htmlByte), -1)
	var codeList []string
	for _, s := range sub {
		if (s[1][2:3] == "6") || (s[1][2:3] == "0") || (s[1][2:3] == "3") {
			codeList = append(codeList, s[1])
		}
	}
	// 加入预设的指数代码列表
	codeList = append(codeList, index_codelist...)

	fmt.Printf("网络下载代码数量:%d\n", len(codeList))
	return codeList

}

// 获取股票列表
func stocklist() []string {
	URL := "http://file.tushare.org/tsdata/h/hq.csv" // 代码下载网址
	var codeList []string
	resp, err := http.Get(URL)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	defer resp.Body.Close()
	buf := bytes.NewBuffer(make([]byte, 0, 512))
	buf.ReadFrom(resp.Body)
	t := string(buf.Bytes())
	Arr := strings.Split(t, ",,")
	for i, s := range Arr {
		arr := strings.Split(s, ",")
		if i == 0 || len(arr) < 10 {
			continue
		}
		//fmt.Println(string(arr[0][2]))
		if arr[0][2] == '6' {
			codeList = append(codeList, "sh"+arr[0][2:])
		} else if arr[0][2] == '0' || arr[0][2] == '3' {
			codeList = append(codeList, "sz"+arr[0][2:])
		} else {
			codeList = append(codeList, arr[0][2:])
		}

	}
	// 加入预设的指数代码列表
	codeList = append(codeList, index_codelist...)
	return codeList
}

// 根据输入生成K线下载的URL(老版本)
func urlGet2(code, startT, endT string) string {
	daySub := daySub(endT, startT) //日期差
	rand.Seed(time.Now().Unix())   //随机数初始化,否则你每次的随机数都是固定的顺序
	url := "http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline_dayqfq&param=%s,day,%s,%s,%d,qfq&r=%f"
	URL := fmt.Sprintf(url, code, startT, endT, daySub, rand.Float64())
	return URL
}

// 根据输入生成K线下载的URL
func urlGet(code, startT, endT string) []string {
	var URLlist []string
	daylist := daySplit(endT, startT, 800)
	for _, item := range daylist {
		st := item[0]
		ed := item[1]
		dsub := item[2]
		url := "http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline_dayfq&param=%s,day,%s,%s,%s,fq&r=%f"
		URL := fmt.Sprintf(url, code, st, ed, dsub, rand.Float64())
		URLlist = append(URLlist, URL)
	}
	return URLlist
}

// 下载日K线数据（老版本）
func get_k_data2(code, startT, endT string) [][]string {
	//---------------------------------生成下载的URL
	url := urlGet2(code, startT, endT)
	//---------------------------------获取的数据结构
	type stockStruct struct {
		Code int `json:"code"`
		Data struct {
			Value struct {
				Qfqday [][]string `json:"qfqday"`
			} `json:"value"`
		} `json:"data"`
		Msg string `json:"msg"`
	}
	//---------------------------------下载
	resp, err := http.Get(url)
	check(err)
	defer resp.Body.Close()
	buf := bytes.NewBuffer(make([]byte, 0, 512))
	buf.ReadFrom(resp.Body)
	var jsonByte = buf.Bytes()[13:]

	// 更换json中的shxxxxx字段为value([]byte->string->[]byte)
	jsonStr_r := strings.Replace(string(jsonByte), code, "value", -1)
	// 如果是指数，还要将其中的day字段换成qfqday,如果不是则更换不起作用
	jsonStr_r = strings.Replace(jsonStr_r, `"day"`, `"qfqday"`, -1)
	// 转换回byte
	jsonByte_r := []byte(jsonStr_r)

	// 解码json->struct
	ff := stockStruct{}
	json.Unmarshal(jsonByte_r, &ff)
	// 取出数据
	DataStruct := ff.Data.Value.Qfqday
	// 此步是为了去除复权日多出来的数据
	var DS [][]string
	for _, row := range DataStruct {
		if len(row) > 6 {
			row = row[0:6]
		}
		row = append(row, code)
		DS = append(DS, row)
	}

	return DS

}

// 下载日K线数据
func get_k_data(code, startT, endT string) [][]string {
	var DS [][]string
	//---------------------------------生成下载的URL
	urllist := urlGet(code, startT, endT)
	//---------------------------------获取的数据结构
	for _, url := range urllist {
		type stockStruct struct {
			Code int `json:"code"`
			Data struct {
				Value struct {
					Day [][]string `json:"day"`
				} `json:"value"`
			} `json:"data"`
			Msg string `json:"msg"`
		}
		//---------------------------------下载
		resp, err := http.Get(url)
		check(err)
		defer resp.Body.Close()
		buf := bytes.NewBuffer(make([]byte, 0, 512))
		buf.ReadFrom(resp.Body)
		var jsonByte = buf.Bytes()[12:]

		// 更换json中的shxxxxx字段为value([]byte->string->[]byte)
		jsonStr_r := strings.Replace(string(jsonByte), code, "value", -1)
		// 转换回byte
		jsonByte_r := []byte(jsonStr_r)
		// 解码json->struct
		ff := stockStruct{}
		json.Unmarshal(jsonByte_r, &ff)
		// 取出数据
		DataStruct := ff.Data.Value.Day
		// 此步是为了去除复权日多出来的数据
		for _, row := range DataStruct {
			if len(row) > 6 {
				row = row[0:6]
			}
			row = append(row, code)
			DS = append(DS, row)
		}

	}

	return DS

}

// 获取复权数据
func get_fq_data(code string) {
	URL := fmt.Sprintf("http://data.gtimg.cn/flashdata/hushen/fuquan/%s.js?maxage=6000000", code)
	resp, err := http.Get(URL)
	check(err)
	defer resp.Body.Close()
	buf := bytes.NewBuffer(make([]byte, 0, 512))
	buf.ReadFrom(resp.Body)
	str := buf.String()
	strArr := strings.Split(str[15:len(str)-2], "^")
	var fqArr []float64
	var result [][]string
	for _, istr := range strArr {
		istrArr := strings.Split(istr, "~")
		istrArr[0] = istrArr[0][0:4] + "-" + istrArr[0][4:6] + "-" + istrArr[0][6:]
		fq, _ := strconv.ParseFloat(istrArr[1], 64)
		fqArr = append(fqArr, fq)
		istrArr[3], _ = strconv.Unquote(`"` + istrArr[3] + `"`)
		istrArr = append(istrArr, []string{"0", "0"}...)
		result = append(result, istrArr)
	}
	z := float64(1)
	f := z
	L := len(fqArr)
	var z_fqMult []string
	f_fqMult := z_fqMult

	for i, v := range fqArr {
		z = z * v
		f = f * fqArr[L-i-1]
		result[i][5] = append(z_fqMult, strconv.FormatFloat(z, float64, 'E', -1, 64))
		result[L-1-i][6] = append(f_fqMult, strconv.FormatFloat(f, float64, 'E', -1, 64))
	}
	fmt.Println(istrArr)
}

//将数组存储道postgresql(全部数据一条insert语句)
func to_psql(value [][]string, tx *sql.Tx) {
	// 将数组直接变成形如(a,b,c),(d,e,f) 形式的字符串，直接结合insert语句生成插入长字符串，效率高
	v := strings.Replace(strings.Replace(strings.Replace(strings.Replace(fmt.Sprint(value), " ", "','", -1), "[", "('", -1), "]", "')", -1), ")','(", "),(", -1)
	vv := v[2 : len(v)-2]
	sqlStr := fmt.Sprintf("insert into golang values %s;", vv)
	_, err := tx.Exec(sqlStr)
	check(err)

}

//并发相关========================================================================

// -----------------------方案1（下载与存储并行）
// 生产者函数（下载）
func producer(c chan []string, v chan [][]string, pname int) {

	var cValue []string   // 待下载通道取数的中间变量
	var code, s, e string // 代码，开始日，结束日

	// 循环下载数据
	rand.Seed(time.Now().Unix()) //随机数初始化,否则你每次的随机数都是固定的顺序
	for {
		// 无限循环，等到待下载通道为空时候退出
		if len(c) == 0 {
			cDone = true
			break
		}
		// 取出待下载参数
		cValue = <-c
		code, s, e = cValue[0], cValue[1], cValue[2]
		// 下载数据
		value := get_k_data(code, s, e)
		// 下载数据不为空则传到给消费者
		if len(value) != 0 {
			v <- value
		}
		fmt.Printf("下载数据 %s（%s-%s）__线程 %d ----------------- \n", code, s, e, pname)
	}
	// 全部下载完成后，往阻塞通道里放入数据以释放此线程的阻塞
	pFinish <- 0
	fmt.Printf(">>下载线程%d结束<< \n", pname)
}

// 消费者函数（存储）
func consumer(v chan [][]string, db *sql.DB, cname int) {
	hasMore := true
	var value [][]string //
	var valueAll [][]string
	count := 0
	tx, err := db.Begin()

	// 循环存储开始
	for hasMore {

		if count > storeL {
			err = tx.Commit()
			check(err)
			tx, _ = db.Begin()
			count = 0
			fmt.Printf(" %d 存储线程提交了一次 \n", cname)
		}
		if !cDone && len(v) == 0 {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		if cDone && len(v) == 0 {
			err = tx.Commit()
			check(err)
			break
		}

		if value, hasMore = <-v; hasMore {
			to_psql(value, tx)
			count = count + len(value)
			fmt.Printf("-----------------存储数据%s__线程 %d \n", value[0][6], cname)
		}

	}
	cFinish <- 0
	fmt.Printf(">>存储线程%d结束<< %d \n", cname, len(valueAll))

}

// 主调用函数
func downlolad() {

	t1 := time.Now()                 // 计时开始
	pFinish = make(chan int, pCount) // 下载阻塞信道，缓冲大小为下载线程个数(全局)
	cFinish = make(chan int, pCount) // 存储阻塞信道，缓冲大小为存储线程个数(全局)
	c := make(chan []string, L)      // 代码信道
	v := make(chan [][]string, L)    // 值信道
	// ----------------------------------------数据库启动
	db := getDB("postgresql") //连接数据库
	defer db.Close()
	// ----------------------------------------检测表是否存在或合规
	if !checkTable(db, daily_table_name) {
		createTable(db, daily_table_name)
		fmt.Sprintf("重建%s表\n", daily_table_name)
	}

	// ---------------------------------------cover 或 append 的相关工作
	cl := stocklist()[0:L] //网上下载代码列表

	// 如果store_type为cover,则清空原表
	if storeType == "cover" {
		_, err := db.Query(fmt.Sprintf("truncate table %s", daily_table_name))
		check(err)
	}
	// 如果store_type为append，则取得所有已经下载数据的时间范围
	if storeType == "append" {
		DL_date = read_lastDate(db)
	}

	// ---------------------------------------代码列表(代码，开始日，结束日)，存储到一个缓冲线程中，供下载线程调用

	for _, code := range cl {
		if storeType == "append" {
			s, e, ok := dateRange(code)
			if ok {
				c <- []string{code, s, e}
				fmt.Println("存放入C(append)：", code, s, e)
			} else {
				fmt.Println("无需存放入C：", code)
				continue
			}
		} else {
			c <- []string{code, startD, endD}
			fmt.Println("存放入C(cover)：", code, startD, endD)
		}

	}
	close(c) // 关闭代码列表信道，变成只读信道
	fmt.Println("代码存放完毕=======================================")
	// ---------------------------------------下载线程启动
	// 下载线程开始
	for i := 0; i < pCount; i++ {
		go producer(c, v, i)
	}

	//----------------------------------------//存储线程启动

	// 设定可异步提交
	_, err := db.Query("set synchronous_commit to off")
	check(err)
	// 删除可能存在的索引
	_, err = db.Query(fmt.Sprintf("drop index if exists %s", index_name[daily_table_name]))
	check(err)

	for i := 0; i < cCount; i++ {
		go consumer(v, db, i)
	}

	// ---------------------------------------等待各个下载和存储线程运行完毕放入数据，打开阻塞线程，使主线程继续
	// ---------------------下载阻塞
	for i := 0; i < pCount; i++ {
		<-pFinish
	}
	// 下载计时结束
	fmt.Println("===============================================下载计时: ", time.Since(t1))

	// ---------------------存储阻塞

	for i := 0; i < cCount; i++ {
		<-cFinish
	}
	// 建立日线表索引
	_, err = db.Query(fmt.Sprintf("create index %s on %s(%s)", index_name[daily_table_name], daily_table_name, array2str(index_colume[daily_table_name], ",")))
	check(err)
	// 下载+存储计时结束
	fmt.Println("===============================================下载+存储+建立索引 计时: ", time.Since(t1))

	// --------------------------------------------------------------生成stock_code表及其索引
	if !checkTable(db, code_table_name) {
		createTable(db, code_table_name)
	} else {
		_, err = db.Exec(fmt.Sprintf("drop index if exists %s", index_name[code_table_name]))
		check(err)
		_, err = db.Exec(fmt.Sprintf("truncate table %s", code_table_name))
		check(err)
	}

	sqlStr := fmt.Sprintf("insert into %s select code as %s,min(date) as %s,max(date) as %s from %s group by code", code_table_name, columeName[code_table_name][0], columeName[code_table_name][1], columeName[code_table_name][2], daily_table_name)
	_, err = db.Exec(sqlStr)
	sqlStr = fmt.Sprintf("create index %s on %s(%s)", index_name[code_table_name], code_table_name, array2str(index_colume[code_table_name], ","))
	_, err = db.Exec(sqlStr)
	check(err)
	// 下载+存储+建立附表计时结束
	fmt.Println("===============================================下载+存储+建立索引+建立附表 计时: ", time.Since(t1))
}

//test==============================================================================

// main=============================================================================
func main() {
	//initialize()
	//downlolad()
	get_fq_data("sh600123")

}
