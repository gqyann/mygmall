package com.atguigu.app

import java.text.SimpleDateFormat

import org.apache.phoenix.spark._
import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {
  def main(args: Array[String]): Unit = {
    // TODO 1 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    // TODO 2 利用SparkConf创建Streaming Sontext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //获取Kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        val ts: Long = startUpLog.ts

        val dateAndHourStr: String = sdf.format(ts)
        startUpLog.logDate = dateAndHourStr.split(" ")(0)
        startUpLog.logHour = dateAndHourStr.split(" ")(1)

        startUpLog
      })
    })

    // 5. 跨批次去重
//    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByReids(startUpLogDStream)
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByReids(startUpLogDStream, ssc.sparkContext)

    startUpLogDStream.cache()
    filterByRedisDStream.cache()

    startUpLogDStream.count().print()
    filterByRedisDStream.count().print()


    // 6. 批次内去重
    val filterByMidDStream: DStream[StartUpLog] = DauHandler.filterByMid(filterByRedisDStream)

    filterByMidDStream.cache()
    filterByMidDStream.count().print()


    // 7. 将去重后的数据写入redis
    DauHandler.saveMidToRedis(filterByMidDStream)


    // 8. 将明细数据写入hbase
    filterByMidDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("gmall0923_dau",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //打印从kafka读取的日志
/*    kafkaDStream.foreachRDD(rdd => {
      rdd.foreach(record => {
        println(record.value())
      })
    })*/

    // TODO 3 启动streamingContext并一直运行
    //开启任务
    ssc.start()
    //阻塞任务
    ssc.awaitTermination()
  }

}
