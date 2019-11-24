// psql
package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/Unknwon/goconfig"
	_ "github.com/lib/pq"
)

//========================================================================函数

// 初始化函数
func initialize() {
	// 读取.ini文件
	readIni()
	// 表设计
	columeName[daily_table_name] = []string{"date", "open", "close", "high", "low", "volume", "code"} //日线表 列名
	columeType[daily_table_name] = []string{"date", "real", "real", "real", "real", "real", "text"}   //日线表 列属性
	index_name[daily_table_name] = "daily_index"                                                      //日线表 索引名称
	index_colume[daily_table_name] = []string{"code", "date"}                                         //日线表 索引列

	columeName[code_table_name] = []string{"code", "star_date", "end_date"} //代码表 列名
	columeType[code_table_name] = []string{"text", "date", "date"}          //代码表 列属性
	index_name[code_table_name] = "codeinfo_index"                          //代码表 索引名称
	index_colume[code_table_name] = []string{"code"}                        //代码表 索引列

	columeName[fq_table_name] = []string{"date", "iqfq", "ihfq", "info", "qfq", "hfq", "code"}   //代码表 列名
	columeType[fq_table_name] = []string{"date", "real", "real", "text", "real", "real", "text"} //代码表 列属性
	index_name[fq_table_name] = "fq_index"                                                       //代码表 索引名称
	index_colume[fq_table_name] = []string{"code", "date"}                                       //代码表 索引列

	if L == -1 {
		L = len(stocklist())
	}

}

// 读取.ini 文件
func readIni() {
	//读取ini文件
	cfg, err := goconfig.LoadConfigFile("conf.ini")
	if err != nil {
		check(err)

	}
	//-------------------------------[Postgresql]

	host_psql, err = cfg.GetValue("Postgresql", "host_psql")
	if err != nil {
		check(err)
	}
	port_psql, err = cfg.Int("Postgresql", "port_psql")
	if err != nil {
		check(err)
	}
	user_psql, err = cfg.GetValue("Postgresql", "user_psql")
	if err != nil {
		check(err)
	}
	password_psql, err = cfg.GetValue("Postgresql", "password_psql")
	if err != nil {
		check(err)
	}
	dbname_psql, err = cfg.GetValue("Postgresql", "dbname_psql")
	if err != nil {
		check(err)
	}
	//-------------------------------[Download_DateRange]

	startD, err = cfg.GetValue("Download_DateRange", "startD")
	if err != nil {
		check(err)
	}
	endD, err = cfg.GetValue("Download_DateRange", "endD")
	if err != nil {
		check(err)
	}
	storeType, err = cfg.GetValue("Download_DateRange", "storeType")
	if err != nil {
		check(err)
	}
	storeContent, err = cfg.GetValue("Download_DateRange", "storeContent")
	if err != nil {
		check(err)
	}
	//-------------------------------[Thread]

	pCount, err = cfg.Int("Thread", "pCount")
	if err != nil {
		fmt.Println(err)

	}
	cCount, err = cfg.Int("Thread", "cCount")
	if err != nil {
		fmt.Println(err)

	}
	//-------------------------------[Custom]

	L, err = cfg.Int("Custom", "L")
	if err != nil {
		fmt.Println(err)

	}
	storeL, err = cfg.Int("Custom", "storeL")
	if err != nil {
		fmt.Println(err)

	}

}

// 获取数据库连接
func getDB(DBname string) *sql.DB {
	var db *sql.DB
	switch DBname {
	case "postgresql":
		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
			"password=%s dbname=%s sslmode=disable",
			host_psql, port_psql, user_psql, password_psql, dbname_psql)
		db, err := sql.Open("postgres", psqlInfo)
		if err != nil {
			panic(err)
		}

		err = db.Ping()
		if err != nil {
			panic(err)
		}
		fmt.Println("%s Successfully connected!", DBname)

		return db
	default:
		fmt.Println("没有这个数据库模块")
		return db

	}
}

// 执行带value的sql语句
func execSql(tx *sql.Tx, sqlStr string, value []string) {
	stmt, err := tx.Prepare(sqlStr)
	check(err)
	//res, err := stmt.Exec(value[0], value[1], value[2], value[3], value[4], value[5], value[6])
	_, err = stmt.Exec(value[0], value[1], value[2], value[3], value[4], value[5], value[6])

	check(err)
	//id, err := res.RowsAffected() //.LastInsertId()//
	//check(err)
	//fmt.Println(id)
	stmt.Close()
}

// 检测有没有与预设相同的表
func checkTable(db *sql.DB, tableName string) bool {
	in_tablelist := false
	for _, tb := range table_name_list {
		if tableName == tb {
			in_tablelist = true
			break
		}
	}
	if !in_tablelist {
		fmt.Printf("表格 %s 的预设不存在！\n", tableName)
		return in_tablelist
	}

	rows, err := db.Query("select tablename from pg_tables where schemaname='public'")
	check(err)
	result := false
	var row string
	for rows.Next() {
		err := rows.Scan(&row)
		check(err)
		if row == tableName {
			result = true
			break
		}
	}
	if !result {
		fmt.Printf("%s 中没有表格 %s 存在！\n", dbname_psql, tableName)
		return result
	}

	rows, err = db.Query(fmt.Sprintf(`SELECT
											column_name
											,data_type
									  FROM information_schema.columns
									  WHERE table_schema = 'public' and table_name='%s'`, tableName))

	var colume_name, data_type string
	i := -1
	for rows.Next() {
		i++
		if i+1 > len(columeName[tableName]) {
			fmt.Printf("表格%s列数与预设不符,预设为%d列，实际已经超过\n", tableName, len(columeName[tableName]))
			return false
		}
		err := rows.Scan(&colume_name, &data_type)
		check(err)

		if columeName[tableName][i] != colume_name || columeType[tableName][i] != data_type {
			fmt.Printf("表格%s中第%d列与预设不符合,预设为%s(%s),实际为%s(%s)，\n", tableName, i+1, columeName[tableName][i], columeType[tableName][i], colume_name, data_type)
			return false
		}

	}
	if i+1 != len(columeName[tableName]) {
		fmt.Printf("表格%s列数与预设不符,预设为%d列,实际为%d列\n", tableName, len(columeName[tableName]), i+1)
		return false
	}
	return true
}

// 创建或替换表
func createTable(db *sql.DB, tableName string) bool {
	in_tablelist := false
	for _, tb := range table_name_list {
		if tableName == tb {
			in_tablelist = true
			break
		}
	}
	if !in_tablelist {
		fmt.Printf("表格 %s 的预设不存在！\n", tableName)
		return in_tablelist
	}
	strBody := ""
	strHeader := fmt.Sprintf("create table %s(\n", tableName)
	strTail := ")"
	for i, _ := range columeName[tableName] {
		strBody = strBody + columeName[tableName][i] + " " + columeType[tableName][i] + ",\n"
	}
	sqlStr := strHeader + strBody[0:len(strBody)-2] + strTail

	_, err := db.Exec(fmt.Sprintf("drop table if exists %s CASCADE", tableName))
	check(err)
	_, err = db.Exec(sqlStr)
	check(err)
	return true
}

// 读取stock_code表到map中
func read_lastDate(db *sql.DB) map[string][]string {
	reslut := make(map[string][]string)
	rows, err := db.Query("select code,min(date),max(date) from golang group by code")
	check(err)
	var code, firstDate, lastDate string
	for rows.Next() {
		err := rows.Scan(&code, &firstDate, &lastDate)
		check(err)
		reslut[code] = []string{firstDate[0:10], lastDate[0:10]}

	}
	return reslut
}

// 返回每个code需要下载的日期区间
func dateRange(code string) (st, ed string, ok bool) {
	if _, ok := DL_date[code]; ok {
		if daySub(endD, DL_date[code][1]) > 0 {
			e, _ := time.Parse("2006-01-02", DL_date[code][1])
			st := e.AddDate(0, 0, 1).String()[0:10]
			ed := endD
			return st, ed, true
		} else {
			return "", "", false
		}
	} else {
		return startD, endD, true
	}
}
