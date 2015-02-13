package org.apache.spark.sql.execution

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.FunSuite

class DiskPartitionSuite extends FunSuite {

  // TESTS FOR TASK #1
  test ("disk partition") {
    val partition: DiskPartition = new DiskPartition("disk partition test", 2000)

    for (i <- 1 to 500) {
      partition.insert(Row(i))
    }

    partition.closeInput()

    val data: Array[Row] = partition.getData.toArray
    (1 to 500).foreach((x: Int) => assert(data.contains(Row(x))))
  }

  test ("close input") {
    val partition: DiskPartition = new DiskPartition("close input test", 1)

    intercept[SparkException] {
      partition.getData()
    }

    partition.closeInput()

    intercept[SparkException] {
      partition.insert(Row(1))
    }
  }

  test("no input"){
    val partition: DiskPartition = new DiskPartition("input test", 64000)
    partition.closeInput()
    val data: Iterator[Row] = partition.getData()
    assert(data.hasNext==false)
  }

  test("irregular use"){
    val partition: DiskPartition = new DiskPartition("input test", 64000)
    val tuple: Row = Row(1)
    partition.insert(tuple)
    partition.closeInput()
    val data: Iterator[Row] = partition.getData()
    assert(data.next()==tuple)
  }
}
