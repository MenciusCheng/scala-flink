package org.example

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet, createTypeInformation}

object DataSetJob {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val inputDataSet: DataSet[String] = env.readTextFile("C:\\Users\\menci\\devProjects\\flink-session\\words.txt")

    val wordDataSet: DataSet[String] = inputDataSet.flatMap(_.split(" "))

    val tupleDataSet: DataSet[(String, Int)] = wordDataSet.map((_,1))

    val groupedDataSet: GroupedDataSet[(String, Int)] = tupleDataSet.groupBy(0)

    val resultDataSet: DataSet[(String, Int)] = groupedDataSet.sum(1)

    resultDataSet.print()
  }
}
