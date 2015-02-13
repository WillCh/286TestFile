package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{Expression, Attribute, ScalaUdf, Row}
import org.apache.spark.sql.catalyst.types.{IntegerType,StringType,FloatType}
import org.scalatest.FunSuite

import scala.collection.mutable.ArraySeq
import scala.util.Random

case class Student(sid: Int, gpa: Float)
case class CoolStudent(sid: Int, gpa: String)

class CS186UtilsSuite extends FunSuite {
  val numberGenerator: Random = new Random()

  // TESTS FOR TASK #3
  /* NOTE: This test is not a guarantee that your caching iterator is completely correct.
     However, if your caching iterator is correct, then you should be passing this test. */
  var studentAttributes: Seq[Attribute] =  ScalaReflection.attributesFor[Student]
  test("caching iterator") {
    val list: ArraySeq[Row] = new ArraySeq[Row](1000)

    for (i <- 0 to 999) {
      list(i) = (Row(numberGenerator.nextInt(10000), numberGenerator.nextFloat()))
    }


    val udf: ScalaUdf = new ScalaUdf((sid: Int) => sid + 1, IntegerType, Seq(studentAttributes(0)))

    val result: Iterator[Row] = CachingIteratorGenerator(studentAttributes, udf, Seq(studentAttributes(1)), Seq(), studentAttributes)(list.iterator)

    assert(result.hasNext)

    result.foreach((x: Row) => {
      val inputRow: Row = Row(x.getInt(1) - 1, x.getFloat(0))
      assert(list.contains(inputRow))
    })
  }

  studentAttributes =  ScalaReflection.attributesFor[CoolStudent]
  test("caching iterator2") {
    val list: ArraySeq[Row] = new ArraySeq[Row](1000)

    for (i <- 0 to 999) {
      list(i) = (Row(numberGenerator.nextInt(10000), numberGenerator.nextFloat().toString()))
    }


    val udf: ScalaUdf = new ScalaUdf((gpa: String) => gpa +"lol", StringType, Seq(studentAttributes(1)))

    val result: Iterator[Row] = CachingIteratorGenerator(studentAttributes, udf, Seq(studentAttributes(0)), Seq(), studentAttributes)(list.iterator)

    assert(result.hasNext)

    result.foreach((x: Row) => {
      val inputRow: Row = Row(x.getInt(0) , x.getString(1).dropRight("lol".length))
      assert(list.contains(inputRow))
    })
  }

  test("sequence with 0 UDF"){
    val attributes: Seq[Expression] = Seq() ++ studentAttributes++ Seq()

    assert(CS186Utils.getUdfFromExpressions(attributes) == null)
  }

  test("sequence with 1 UDF") {
    val udf: ScalaUdf = new ScalaUdf((i: Int) => i + 1, IntegerType, Seq(studentAttributes(0)))
    val attributes: Seq[Expression] = Seq() ++ studentAttributes ++ Seq(udf)

    assert(CS186Utils.getUdfFromExpressions(attributes) == udf)
  }

  test("sequence with 2 UDF") {
    val udf1: ScalaUdf = new ScalaUdf((i: Int) => i + 1, IntegerType, Seq(studentAttributes(0)))
    val udf2: ScalaUdf = new ScalaUdf((i: Int) => i + 2, IntegerType, Seq(studentAttributes(0)))

    val attributes: Seq[Expression] = Seq() ++ studentAttributes ++ Seq(udf1) ++ Seq(udf2)

    assert(CS186Utils.getUdfFromExpressions(attributes) == udf2)

    val attributes1: Seq[Expression] = Seq() ++ studentAttributes ++ Seq(udf2) ++ Seq(udf1)

    assert(CS186Utils.getUdfFromExpressions(attributes1) == udf1)
  }
}