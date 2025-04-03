# jndata_power

Flink 任务

## 功能介绍

1. 用户使用了一张 TDengine 超级表 `jndata`.`stb_jndata_realtime`，超级表包含了一组子表，存储来自不同单元的充电量、放电量等指标。这些子表的数据由其他上游应用采集并写入
   TDengine。这组子表称为 CDC 表。

2. 用户使用的超级表 `jndata`.`stb_jndata_realtime`下，还存在一组子表，用于统计各个子单元的数据之和。例如：集团总充电量=
   所有子单元之和，分公司1充电量 = 分公司1所有子单元之和，依此类推。这组子表称为 SUM 表。

3. 单元之间是多级的树状结构，例如：集团 = 公司1 + 公司2 + ...；公司1 = 部门1 + 部门2 + ...，依此类推。这些关系存储在
   达梦数据库中，有管理员手工维护。

4. 目标是，创建一个 Flink 任务，通过 TDengine CDC Source 订阅 CDC 表的数据，每当有数据写入，计算其所有 parent node 的最新
   Sum，计算值写入到 TDengine 的 SUM 表。
