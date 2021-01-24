package org.apache.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, _}
import org.apache.spark.sql.functions.{sum, when, _}
import org.apache.spark.sql.types._

object Logic {

  val conf = new SparkConf().setAppName("hackathon").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.dir.home", "E://DataSamples//hadoop")

    // Reading text files
    val file1 = sc.textFile("C:\\Users\\Sourabh\\IdeaProjects\\Hackathon\\src\\main\\resources\\file1.txt")
    val file2 = sc.textFile("C:\\Users\\Sourabh\\IdeaProjects\\Hackathon\\src\\main\\resources\\file2.txt")

    // header creation
    val fileHeader = "A B C D E"
    val schema = StructType(fileHeader.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    // creating RowRDD
    val rowRDD1 = file1.map(_.split(" ")).map(x => Row(x(0), x(1), x(2), x(3), x(4)))
    val rowRDD2 = file2.map(_.split(" ")).map(x => Row(x(0), x(1), x(2), x(3), x(4)))

    val file1DF = sqlContext.createDataFrame(rowRDD1, schema)
    val file2DF = sqlContext.createDataFrame(rowRDD2, schema)

    file1DF.createOrReplaceTempView("file1")
    var print1 = sqlContext.sql(" select cast(A as int), cast(B as int), cast(C as int), cast(D as int), cast(E as int) from file1 ")
    println("file1.txt Before shifting null values to right :")
    print1.show(6)

    // Shifting null values to right side
    val cols = print1.columns.size
    print1 = print1.withColumn("ct", split(concat_ws("-", print1.columns.map(x => col(x)): _*), "-"))
    print1 = print1.select((0 until cols).map(r => print1.col("ct").getItem(r)): _*).toDF((0 until cols).map(r => print1.columns(r)): _*)
    println("file1.txt After shifting null values to right :")
    print1.show()

    // Shifting null values to right side
    file2DF.createOrReplaceTempView("file2")
    var print2 = sqlContext.sql(" select cast(A as int), cast(B as int), cast(C as int), cast(D as int), cast(E as int) from file2 ")
    println("file2.txt Before shifting null values to right :")
    print2.show(6)

    print2 = print2.withColumn("ct", split(concat_ws("-", print2.columns.map(x => col(x)): _*), "-"))
    print2 = print2.select((0 until cols).map(r => print2.col("ct").getItem(r)): _*).toDF((0 until cols).map(r => print2.columns(r)): _*)
    println("file2.txt After shifting null values to right :")
    print2.show()

    // Appending file1.txt and file2.txt
    val df = print1.union(print2)
    df.show(10)

    // dataframe with sum of individual columns and count of number of non null values in each individual columns
    val sum1 = df.agg(sum("A"), sum("B"), sum("C"), sum("D"), sum("E"), sum(when($"A".isNull, 0).otherwise(1)).as("A"),
      sum(when($"B".isNull, 0).otherwise(1)).as("B"),
      sum(when($"C".isNull, 0).otherwise(1)).as("C"),
      sum(when($"D".isNull, 0).otherwise(1)).as("D"),
      sum(when($"E".isNull, 0).otherwise(1)).as("E"))
    println("dataframe containing sum of each individual column values and count of number of non null values in each column :")
    sum1.show()

    val sum12 = sum1.select(($"sum(A)" + $"sum(B)") / ($"A" + $"B"))
    val sum23 = sum1.select(($"sum(B)" + $"sum(C)") / ($"B" + $"C"))
    val sum34 = sum1.select(($"sum(C)" + $"sum(D)") / ($"C" + $"D"))
    val sum45 = sum1.select(($"sum(D)" + $"sum(E)") / ($"D" + $"E"))


    val AvgValue = sum12.union(sum23).union(sum34).union(sum45).select($"((sum(A) + sum(B)) / (A + B))".alias("Value"))
    println("Avg Values :")
    AvgValue.show()
    var list = List("AvgA&B", "AvgB&C", "AvgC&D", "AvgD&E").toDF("Avg")

    // Creating increasing index value for combining two df
    val AvgValueNew = sqlContext.createDataFrame(
      AvgValue.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index)
      },
      // Create schema for index column
      StructType(AvgValue.schema.fields :+ StructField("index1", LongType, false))
    )
    // Creating increasing index value for combining two df
    val listNew = sqlContext.createDataFrame(
      list.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index)
      },
      // Create schema for index column
      StructType(list.schema.fields :+ StructField("index2", LongType, false))
    )

    val finalDF = listNew.join(AvgValueNew, col("index1").equalTo(col("index2")), "inner").drop("index1", "index2").orderBy($"Avg")
    println("Final Output :")
    finalDF.show(100)

    // Writing to file
    //finalDF.coalesce(1).write.format("com.databricks.spark.csv").save("C:\\Users\\Sourabh\\IdeaProjects\\Hackathon\\src\\main\\resources\\OutputFile")

  }
}
