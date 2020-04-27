package com.traffic.load.data

import java.util.Properties
import java.util.concurrent.Executors

import com.google.gson.Gson
import com.traffic.bean.Info
import com.traffic.util.HbaseUtil
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * FUNCTIONAL_DESCRIPTION:
 * CREATE_BY: 尽际
 * CREATE_TIME: 2019/3/26 10:20
 * MODIFICATORY_DESCRIPTION:
 * MODIFY_BY:
 * MODIFICATORY_TIME:
 * VERSION：V1.0
 */
object StreamingAnalysis {
  final val JDBC_URL = "jdbc:mysql://localhost:3306/traffic?setUnicode=true&characterEncoding=utf8&useSSL=false"
  final val ZK_QUORUM = "172.16.29.106:2181"
  final val GROUP_ID="RoadRealTimeLog"
  final val MYSQL_PASSWORD="root"
  final val MYSQL_USER_NAME="root"
  final val HBASE_TABLE_NAME="real_time_log"

  def writeToMySQL(result: DataFrame, properties: Properties, table: String): Unit = {

    val count = result.count()
    if (count > 0) {
      //可以输出到HDFS、HIVE、HBASE、ES、Redis等外部存储。
      result.write
        .mode(SaveMode.Append)
        .jdbc(
          JDBC_URL,
          table,
          properties)
    }
  }

  def writeToHbase(partition: Iterator[Info]): Unit = {
    //partition和record共同位于本地计算节点Worker，故无需序列化发送conn和statement
    //如果多个分区位于一个Worker中，则共享连接（位于同一内存资源中）
    //获取HBase连接
    val conn = HbaseUtil.getHBaseConn
    if (conn == null) {
      println("conn is null.") //在Worker节点的Executor中打印
    } else {

      partition.foreach((record: Info) => {
        //每个分区中的记录在同一线程中处理

        //设置表名
        val tableName = TableName.valueOf(HBASE_TABLE_NAME)
        //获取表的连接
        val table = conn.getTable(tableName)
        try {
          //设定行键（单词）
          val put = new Put(Bytes.toBytes(record.monitor_time + "_" + record.area_id + "_" + record.monitor_id))
          //添加列值（单词个数）
          //三个参数：列族、列、列值
          put.addColumn(Bytes.toBytes("cf1"),
            Bytes.toBytes("value"),
            Bytes.toBytes(record.camera_id + "," + record.date + "," + record.license + "," + record.road_id + "," + record.speed))
          //执行插入
          table.put(put)

        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          table.close()
        }
      })
    }

  }

  def main(args: Array[String]): Unit = {
    //创建线程池
    val executorService = Executors.newFixedThreadPool(1)
    executorService.submit(new MockRealTimeData)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StreamingAnalysis")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val properties = new java.util.Properties()
    //    properties.setProperty("user", "hive")
    //    properties.setProperty("password", "NEU@pzj123456") //（2）mysql密码
    properties.setProperty("user", MYSQL_USER_NAME)
    properties.setProperty("password", MYSQL_PASSWORD)
    val DURATION = 10

    val ssc = new StreamingContext(spark.sparkContext, Seconds(DURATION))
    val inputDataFrame = KafkaUtils.createStream(ssc, ZK_QUORUM
      , GROUP_ID, Map(GROUP_ID -> 1))
    print("input")
    inputDataFrame.print(10)


    import spark.implicits._

    val keyValueDataset = inputDataFrame.map(_._2)

    val infoDStream: DStream[Info] = keyValueDataset.map(t => {
      val gson = new Gson()
      val info = gson.fromJson(t, classOf[Info])
      info
    })

    infoDStream.foreachRDD(rdd => {
      // RDD为空时，无需再向下执行，否则在分区中还需要获取数据库连接（无用操作
      if (!rdd.isEmpty()) {

        rdd.foreachPartition((partition: Iterator[Info]) => {
          //每个分区都会创建一个task任务线程，分区多，资源利用率高
          //可通过参数配置分区数："--conf spark.default.parallelism=20"

          if (partition.nonEmpty) {
            writeToHbase(partition)
          }
        })
        val infoDS: Dataset[Info] = rdd.toDS()

        infoDS.show()
        infoDS.createOrReplaceTempView("t_info")

        //实时统计各牌照车辆的数量
        //TODO 将license改成了area，不然还是按照完整的牌照进行分组
        val result1 = spark.sql(
          """SELECT
            |  NOW() AS monitor_time, substr(license,1,1) AS area,  COUNT(1) AS count
            |FROM
            |  t_info
            |GROUP BY
            |  area
            |ORDER BY
            |  count
            |DESC
      """.stripMargin)
        println("实时统计各牌照车辆的数量")
        result1.show()

        //实时统计超速车辆
        val result2 = spark.sql(
          """SELECT
            |  monitor_time,monitor_id,camera_id,license,speed,road_id,area_id
            |FROM
            |  t_info
            |WHERE
            |speed>120
      """.stripMargin)
        println("实时统计超速车辆")
        result2.show()

        //统计各个卡口的车流量具体信息
        val result3 = spark.sql(
          """SELECT
            |   monitor_id, COUNT(1) AS count
            |FROM
            |  t_info
            |GROUP BY
            |  monitor_id
            |ORDER BY
            |  count
            |DESC LIMIT
            |  10
      """.stripMargin)
        println("统计各个卡口的车流量具体信息")
        result3.show()

        //实时统计各个区域的车辆数
        val result4 = spark.sql(
          """SELECT
            |  area_id,  COUNT(1) AS count
            |FROM
            |  t_info
            |GROUP BY
            |  area_id
            |ORDER BY
            |  count
            |DESC
      """.stripMargin)

        println("实时统计各个区域的车辆数")
        result4.show()

        //实时统计速度最快的前10辆车
        val result5 = spark.sql(
          """SELECT
            |  monitor_id,license, monitor_time, speed
            |FROM
            |  t_info
            |ORDER BY
            |  speed
            |DESC LIMIT
            |  10
      """.stripMargin)

        println("实时统计速度最快的前10辆车")
        result5.show()

        //写入到mysql

        writeToMySQL(result1, properties, "analyze_car_license")
        writeToMySQL(result2, properties, "over_speed_car")
        writeToMySQL(result3, properties, "monitor_flow")
        writeToMySQL(result4, properties, "area_flow")
        writeToMySQL(result5, properties, "monitor_top_ten")


      }

    })


    ssc.start()
    ssc.awaitTermination()
  }
}

