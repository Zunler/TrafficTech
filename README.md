## 数据

产生的模拟数据发送到Kafka
运行

## 数据存到hbase

实时数据保存到hbase，

1. Rowkey设计:年月日时分秒_区域编号\_卡扣编号;
2. 列:Value (把数据写入一个列中)

## 实时统计

### 字段解释

```
日期        卡口ID         摄像头编号     车牌号           拍摄时间                  车速              道路ID         区域ID
* date  monitor_id     camera_id  car   action_time       speed  road_id       area_id
```

SparkStreaming

每30s统计一次，统计信息结果到mysql

1. 每个区域的车流量具体信息

   area_flow：id,monitor_time,area_id,count

   根据area_id，统计各个区域的车辆

2. 每个卡口的车流量具体信息

   monitor_flow： id,monitor_time,monitor_id，count

3.  所有车辆速度最快的前10名

   monitor_top_ten：id, monitor_time,monitor_id，car，speed MonitorTopTen

4. 统计各牌照车辆的数量

   analyze_car_license: id,monitor_time,license，count

5. 实时监控超速的车辆信息 ：

   over_speed_car: id, monitor_time,monitor_id , camera_id ,license , speed, road_id  ,area_id