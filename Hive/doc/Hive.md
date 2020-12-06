# Hive

>如果数据库中有表,则需要先删除表,在删库
>
>或者 执行强制删库命令
>
>```plsql
>DROP DATABASE IF EXISTS TEST_DATABASE CASCADE
>```



## 外部表 和 内部表

### 外部表

```plsql
create external table test (
 id int,
 name string
)
```

### 内部表

```sql
create table test (
 id int,
 name string
)

-- 查询建表(带数据)
create table table_2 as
select * from table_1;

-- 查询建表(仅数据结构)
create table table_2 like table_1;


-- 建表详情
create table if not exists table_1(
id int comment 'ID',
name String
)
comment 'table_1'
row format delimited -- 指定序列化和反序列化的规则
fields terminated by ',' -- 字段用逗号分隔
lines terminated by '\n' -- 每条数据行 换行 分隔
-- file format 存放在HDFS的文件格式,默认为TEXTFILE,即文本格式
-- localtion 'path' path为 指定表在HDFS的位置
;

-- 加载数据
load data local inpath '/opt/table_1' into table table_2;

```

>外部表被**DROP**掉,其数据还会存在于HDFS中,而内部表都会被删掉,无法恢复;
>
>内部表**Insert**数据,会产生临时表,会话消失时,临时表会消失



## 特殊数据类型

>Array:数组,起始位置为 **0**
>
>```sql
>create table if not exists table_1(
>id int comment 'ID',
>name String,
>likes Array<String>
>)
>row format delimited -- 指定序列化和反序列化的规则
>fields terminated by ',' -- 字段用逗号分隔
>lines terminated by '\n' -- 每条数据行 换行 分隔
>collectio items termainated by '-' -- 集合用'-'分隔
>
>
>select likes[0] from table_1 -- 查找爱好集合的第一个数据
>
>
>explode 将数组拆分成行,
>lateral view  实现聚合
>-- 查询 每个爱好的数量
>select hobby ,count(*) as c1 from table_1 
>lateral view explode(likes) as hobby
>group by hobby 
>order by c1 desc;
>```
>
>
>
>Map:键值对集合,使用数组表示
>
>数据形式 k1:v1-k2:v2-k3:v3 " - 表示单个键值对间隔 以数组的形式表示" 
>
>例如数据:  1,张三,英语:四级-日语:三级
>
>```sql
>create table if not exists table_1(
>id int comment 'ID',
>name String,
>deducts Map<String,String>
>)
>row format delimited -- 指定序列化和反序列化的规则
>fields terminated by ',' -- 字段用逗号分隔
>lines terminated by '\n' -- 每条数据行 换行 分隔
>collection items termainated by '-' -- 集合用'-'分隔
>map keys termainated by ':'-- map 形式为 key:value
>
>
>select deducts[key] from table_1
>
>select explode(deducts) as (e_name,e_level) from table_1
>拆分后 变成
>e_name,e_level 两列
>
>
>select id,name,e_name,e_level from table_1 
>lateral view explode(deducts) deducts as e_name,e_level;
>
>```
>
>
>
>
>
>Struct: 可存放不同类型的集合数据
>
>可以通过 '.'符号访问元素内容
>
>例: 某列数据类型为 struct{班级 String , 学号 Int }
>
>数据 ('9班',11001)
>
>可通过 [列名].`班级` -->得到 '9班'
>
>```sql
>create table if not exists table_1(
>id int comment 'ID',
>address Struct<city:String,province:String,area:String,code:Int>
>)
>row format delimited -- 指定序列化和反序列化的规则
>fields terminated by ',' -- 字段用逗号分隔
>lines terminated by '\n' -- 每条数据行 换行 分隔
>collection items termainated by '-' -- 集合用'-'分隔
>map keys termainated by ':'-- map 形式为 key:value
>
>数据: 
>1,武汉-湖北-东部-100000
>2,大连-辽宁-东北部-116000
>
>select address.city from table_1 -- 查询所有城市
>
>```



| 类型         | Array               | Map                     | Struct                                 |
| ------------ | ------------------- | ----------------------- | -------------------------------------- |
| 列名         | likes               | deducts                 | address                                |
| 声明         | likes array<String> | deducts Map<String,Int> | 见上                                   |
| 文本加载行值 | 足球-电影-游戏      | 英语:120-俄语:60        | 河北-石家庄-10000                      |
| 查询显示行值 | [足球,电影,游戏]    | [英语:120,俄语:60]      | [province:河北,city:石家庄,code:10000] |



## 分区

```sql
create table student(
sno int,
sname string,
sage int
)
partitioned by (sclass string) -- 以班级为分区
row format delimited -- 指定序列化和反序列化的规则
fields terminated by ',' -- 字段用逗号分隔
lines terminated by '\n' -- 每条数据行 换行 分隔
collection items termainated by '-' -- 集合用'-'分隔
map keys termainated by ':'-- map 形式为 key:value

load data local inpath '/opt/syudent' into table student partition (sclass = '12001')

数据文本实例:
1,leo,25
2,marry,23
3,tim,24

-- 添加分区
alter table student add partition  (sclass = '12003')

-- 删除分区 数据也会相应的删除
alter table student drop partition (sclass = '12003')

查询分区表 没有加分区过滤,会禁止提交任务
set hive.mapred.mode = strict; -- 严格模式

set hive.mapred.mode = nostrict; 

select * from student where sclass = '12001'

-- 查看信息
describe extended [table]


```

### 多重分区

结构类似:

- Student
  - 12001班
    - 男
    - 女
  - 12002班
    - 男
    - 女

```sql
create table logs(
id int,
name string,
num int
)
partitioned by (year string,month string,day string) -- 以年月日为分区
row format delimited -- 指定序列化和反序列化的规则
fields terminated by ',' -- 字段用逗号分隔
lines terminated by '\n' -- 每条数据行 换行 分隔
collection items termainated by '-' -- 集合用'-'分隔
map keys termainated by ':'-- map 形式为 key:value


insert into table logs partition (year='2020',month='12',day='01') values (1,'qq')


-- 新增多重分区
alter table logs add partition(year='',month='',day='')
location 'hdfs://name:8020/apps/hive/default.db/logs/'
如果指定到表这一层级 会导致 数据填充 放进 创建的分区


alter table logs add partition(year='2020',month='02',day='12')
location 'hdfs://name:8020/apps/hive/default.db/logs/2020/02/12'
此时数据不会改变

-- 删除多重分区
alter table logs drop partition (month = '02') -- 会删除所有2月的分区

-- 只会删除数据而不删除结构
truncate table logs partition(month = '02')


-- 修改字段
alter table logs change_column num snum string
comment '修改列名称和属性 ,放在 ID 的 后面'
after id;

-- 修改表的存储属性
alter table logs
set fileformat sequencefile;

```



### 动态分区

```sql

create table table1(
id int,
name string,
num int
)
partitioned by (year string,month string,day string) -- 以年月日为分区
row format delimited -- 指定序列化和反序列化的规则
fields terminated by ',' -- 字段用逗号分隔
lines terminated by '\n' -- 每条数据行 换行 分隔
collection items termainated by '-' -- 集合用'-'分隔
map keys termainated by ':'-- map 形式为 key:value


-- 开启动态分区
set hive.exec.dynamic.partition=true

-- strict 严格 模式 至少指定一个分区为静态分区
-- nostrict 非严格模式 可以 让所有分区字段使用动态分区
set hive.exec.dynamic.partition.mode = nostrict


-- 默认值:100
-- 在每个执行MP的节点上，最大可以创健多少个动态分区。
-- 该参数简要根据充际的数据来设定。
-- 比如:源数据中包含了一年的数据，即day字段有365个值。那么该参数就露要设置成大于365，如果使用默认值100，则会报错。
SET hive.exec.max.dynamic.partitions.pernode = 1000;


-- 默认值1000
-- 在所有执行的MP节点上，最大一共可以创建多少个动态分区
-- 解释如上
SET hive.exec.max.dynamic.partitions = 1000;



-- 默认值: 100000
-- 整个MR Job中,最大可以创建多少个HDFS文件。
-- 一般默认值足够了，除非你的数据里非常大。需要创建的文件数大于100000，可根据实际情兄加以调整
hive.exec.max.created.files


-- 默认值: false
-- 当有空分区生成时,是否抛出异常。—般不需要设置。
hive.error.on.empty.partition



insert into table table1 partition (year,month,day)
values(1,'leo','25','2015','1','15')(...)...



```



## 函数

### 关系运算

```sql
select 1 > 2; return false;
select 1 < 2;
select 2 = 2;
select to_date('') > to_date('')

-- 空值判断
select 1 from dual where NULL is NUll LIMIT 1;

select 1 from dual where 1 is not NUll LIMIT 1;

-- 模糊查询
select 1 from dual where '1234' like '123%';
-- '_'下划线表示任意单个字符
select 1 from dual where '1234' like '12_4'; 

--rlike | regexp
--字符串符合 Java正则表达式

```

### 数学运算

```sql
> "+ - * /"
-- hive中最高精度的数据类型是double ,精确到小数点后16位,除法请注意
select 1 + 1;

select 16 / 3;

select 7 % 3;

```

### 逻辑运算

```sql
AND | OR | NOT

select 1=2 and 3=4 or 1=1;

```

### 条件函数

```sql
IF CASE COALESCE
-- COALESCE 返回 参数中 第一个 非空值;如果所有值都为NULL,那么返回NULL

select 
if(4>5,5000,1000), -- return 1000
coalesce(null,1,2,3,4),
coalesce(null,null,null,null),
case 3
when 1 then 'lalala'
when 2 then 'hi'
else 'no' end;


```

### 数值计算

```sql
-- 指定精度 ,四舍五入
round(double a , int e)
select(1.234,2) -- 保留两位小数

ceil(1.234) -- 向上取整 2
floor(1.234) -- 向下取整 1


-- 随机数 rand 返回 [0,1) 内的随机数
rand(int d) 


```





## 索引

虽然也有索引,但是区别于关系型数据库的索引

目的:指在减少某些列上的数据块的大小

若分区数据过去庞大,索引常常是优于分区的

使用场景:

- 不更新的静态字段,以免重建索引数据

索引机制:

hive在指定列上建立索引，会产生一张索引表(hive的一张物理表)，里面的宇段包括，索引列的值、该值对应的HDFS文件路径、该值在文件中的编移量;0.8后引入bitmap索引处理器，这个处理器适用于排重后。值较少的列(例如，某字段的取值只可能是几个枚举值)
因为索引是用空间换时间，索引列拍的取值过多会导致建立bitmap索引表过大。
但是,很少遇到hive用索引的的。说明还是有缺陷或者不合适的地方的.



