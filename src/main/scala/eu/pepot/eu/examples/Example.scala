package eu.pepot.eu.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Example {

  def main(args: Array[String]) {

    if (args.length != 2) {
      sys.error("Expecting inputDirectory outputDirectory")
    }

    val inputDirectory = args(0)
    val outputDirectory = args(1)

    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
      .setAppName("Example")

    implicit val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(inputDirectory)

    val splitLines = lines.map { line =>
      line.split(",")
    }

    val groups = splitLines.groupBy { splitLine =>
      splitLine(0)
    }

    val latests: RDD[String] = groups.flatMap { group =>

      val values = group._2

      val first = values.map(value => value(1)).max

      val concats = values.map { value =>
        if (value(1) == first) {
          ("Y" ++ value).mkString(",")
        } else {
          ("N" ++ value).mkString(",")
        }
      }

      concats

    }

    latests.saveAsTextFile(outputDirectory)

  }

}

