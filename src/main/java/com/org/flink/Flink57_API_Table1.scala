package com.org.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{Table, TableEnvironment}

object Flink57_API_Table1 {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val dataDS: DataStream[String] = env.readTextFile("input/sensor.txt")

        val sensorDS: DataStream[WaterSensor] = dataDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )

        // TODO 使用Table API

        // 获取TableAPI环境
        val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
        // 将数据转换为一张表并进行简单的过滤
        val table: Table = tableEnv.fromDataStream(sensorDS, 'ts)

        val result: DataStream[Long] = table.toAppendStream[Long]

        result.print("table>>>")

        env.execute()
    }

}
