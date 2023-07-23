package org.example

import org.apache.flink.streaming.api.scala._

object StreamWorldCount {
  def main(args: Array[String]): Unit = {
    // Sets up the execution environment, which is the main entry point
    // to building Flink applications.
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data: DataStream[String] = env.socketTextStream("localhost", 9999)

    val result: DataStream[(String,Int)] = data.flatMap(_.split(" "))
      .filter(_.nonEmpty).map((_, 1))
      .keyBy(_._1)
      .sum(1)

    result.print()

    env.execute("StreamWorldCount")
  }
}
