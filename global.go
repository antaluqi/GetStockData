// global
package main

//---------------Postgresql 数据库登陆参数
var host_psql = "localhost"
var port_psql = 5432
var user_psql = "postgres"
var password_psql = "123456"
var dbname_psql = "testDB"
var daily_table_name = "golang"
var code_table_name = "stock_code"
var fq_table_name = "fq"
var table_name_list = []string{daily_table_name, code_table_name, fq_table_name}

// ------------------------表信息

var columeName = make(map[string][]string)
var columeType = make(map[string][]string)
var index_name = make(map[string]string)
var index_colume = make(map[string][]string)

//-------------------------下载起始日
var startD string                       //开始日
var endD string                         //结束日
var DL_date = make(map[string][]string) //用于append的每个code下载起始日储存
var storeType string                    // 重置reload/更新append
var pCount int                          // 下载线程数
var cCount int                          // 存储线程数
var L int                               // 测试代码的个数
var storeL int                          // 每次事务提交的长度
var pFinish chan int                    // 下载线程结束标志（通道阻塞）
var cFinish chan int                    // 存储线程结束标志（通道阻塞）
var cDone = false                       // 代码读取结束标志
var resultArr [][]string

//-------------------------指数代码列表
var index_codelist = []string{"sh000001", "sh000002", "sh000003", "sh000008", "sh000009",
	"sh000010", "sh000011", "sh000012", "sh000016", "sh000017",
	"sh000300", "sh000905", "sz399001", "sz399002", "sz399003",
	"sz399004", "sz399005", "sz399006", "sz399008", "sz399100",
	"sz399101", "sz399106", "sz399107", "sz399108", "sz399333",
	"sz399606"}

/*
update golang set open =case code
       when 'sh600118' then open+100
	   when 'sh600008' then open-100
	   when 'sh600525' then open-500
	   else open
	   end
*/
/*
create table golang(
	date date,
	open real,
	high real,
	close real,
	low real,
	volume real,
	code text
)
*/

/*
select a.*,a.id+b.addNum as id2 from
golang a,
(select code,1-min(id) as addNum from  golang group by code) b
where a.code=b.code



update golang a set id=a.id+b.addNum from
(select code,1-min(id) as addNum from  golang group by code) b
where a.code=b.code
*/

/*

create or replace function lastline(n integer) returns setof golang as $$
declare
x text;
begin
   for x in select code from stock_code loop
	     return query select * from golang where code=x order by date desc limit n;
    end loop;
return;
end;
$$ language plpgsql strict;
CREATE FUNCTION


*/
