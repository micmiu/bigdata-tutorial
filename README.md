# bigdata-tutorial #

大数据相关技术的学习示例

## hadoop2-demo ##
 hadoop mr jobs demo
 + WordCount 			: 官方经典的MR演示
 + XflowDstIPCount  	: 分析netflow 目的IP的工具类
 + XflowStatic   		: 分析统计netflow 中五元组
 + Mapper2HbaseDemo 	: 利用MR的mapper执行数据分析入库到hbase，reduce task 数量设置为0
 + MRLogDemo  			: 测试MR中各种日志(log 、system.out)的输出
 + MRUseLibjarsDemo 	: 参数-libjars 配置第三方jar包的演示
 + MapperInputSplitInfo : 示例如何在mapper处理阶段中获取当前正在处理的HDFS文件名
 + MrjobRemoteCommitDemo: 远程提交 MRjob 到集群运行


## hive-demo ##
 Hive extends demo:
 + MyDemoInputFormat	: 自定义字符串作为分隔符
 + HiveConnDbcpManager  : DBCP实现Hive连接池
 + JSONCDHSerDe         : SerDe for JSON

## es-tutorial ##
 elasticsearch query demo.
 
## hbase-tutorial ##
 Hbase API demos:
 + HBaseConnPoolManager  : 连接池
 + HBaseDDLHandler       : HBase Java demo for DDL
 + HBaseDMLHandler       : HBase Java demo for DML
 + RegionObserverDemo    : HBase coprocessor demo
 + EnvViewer             : HBase coprocessor for get environment
 
## ...... ##


