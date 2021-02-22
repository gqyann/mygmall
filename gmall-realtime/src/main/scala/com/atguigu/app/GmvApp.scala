package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyKafkaUtil, OrderInfo}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object GmvApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GmvAPP").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //获取Kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    //将数据转化为样例类，补全数据
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //转化为样例类
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        //"create_time":"2020-02-22 09:40:33"
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)

        //手机号脱敏
        orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 3) + "********"

        orderInfo
      })
    })

    import org.apache.phoenix.spark._
    orderInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL0923_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL",
          "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT",
          "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME",
          "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}

