package com.org.flink

import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.joda.time.LocalDateTime.Property

object Atest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val properties = new java.util.Properties()
        properties.setProperty("bootstrap.servers", "hadoop102:9092")
        properties.setProperty("group.id", "consumer-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")
        val fileDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String](
            "log",
            new SimpleStringSchema(),
            properties
        ))

        val flatDS: DataStream[String] = fileDS.flatMap(
            line=>{
                line.split(" ")
            }
        )

        val mapDS: DataStream[(String, Int)] = flatDS.map((_, 1))

        val keyDS: KeyedStream[(String, Int), Tuple] = mapDS.keyBy(0)

        val result: DataStream[(String, Int)] = keyDS.sum(1)

        result.print()
        env.execute()
    }
}
