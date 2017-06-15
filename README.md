# BinlogUpdatetoHive
mysql近实时增量导数据到Hive
背景
-------
目前，我公司大数据团队，使用的数据仓库是Hive。
客户端开发团队，使用的数据库是Mysql，客户端上的各种数据都存储在了Mysql中。
每天凌晨，大数据团队会批量把Mysql从库的全部数据导入Hive。 
此方案存在的问题是：
1.	每天全量导入数据耗时较长，且对集群消耗较大。
2.	Hive中数据每天更新一次，时效性较低。
3.	在批量导入过程中，不能对Hive进行任何操作。

需要解决的问题是：
------
1.	弃用全量导数据的方式，采用增量导数据。
2.	Hive中数据要近实时更新，包括Mysql表中的增删改数据。
3.	导数据过程中，可以对Hive进行各种操作，且保证数据的一致性。
  
已有的解决方案及不足：
------
1. 使用Sqoop，依据Mysql表每条记录中的修改时间戳字段，进行增量导入。
上述解决方案的优势是，简单，易操作，不需要使用新的组件。
2. Oracle等公司有针对数据库之间数据同步的成套解决方案，包括Mysql到Hive的增量数据导入。

方案1的不足是：
1.	需要Mysql表中有修改时间戳字段。
2.	增量导入，不能做到近实时性。
方案2的不足是：
1.	需要购买Oracle公司的整套解决方案，费用昂贵。
2.	解决方案不是开源的，维护起来比较困难。

解决方案
------
Mysql的Binlog日志，记录了Mysql表的增删改查日志。
使用阿里开源的Canal工具，可以实时获取和解析Mysql的Binlog日志。
根据解析后的Binlog日志，可以对Hive进行相应的增删改操作。
Hive 0.8版本之后，支持事务性的增删改。

方案中可能会遇到的难题
------
在此技术方案中，可能会遇到的技术难题是：
Hive中没有主键的概念，Hive使用事务Id+批次Id来唯一标识一条记录，对Hive中每条记录的增删改操作，Hive都会分配唯一的事务Id+桶Id+批次Id。
当发生增删改操作时，需要根据Mysql表的主键，查找到Hive表中对应的记录，即查找到对应的事务Id+批次Id。
当对Hive进行插入数据操作之后，需要存储事务Id+桶Id+批次Id，Hive原生的方法是将此组合Id存储在Hive表中，这样会造成查找时候的效率低下，严重影响增删改操作的性能。针对这一点的解决方法是，将Hive的事务Id+桶Id+批次Id存储在Hbase中，Hbase支持实时查询，这样可以大幅减少查找的时间。

整体架构
-----
 ![image](https://github.com/xiaofateng/BinlogUpdatetoHive/tree/master/images/mysql到hive增量导数据架构图.png)

阿里的canal开源工具
开源工具介绍：https://github.com/alibaba/canal

hive2事务性介绍
http://blog.csdn.net/maixia24/article/details/73250155

Streaming Data Ingest介绍
http://blog.csdn.net/maixia24/article/details/73250305

HCatalog Streaming Mutation API
http://blog.csdn.net/maixia24/article/details/73250424





