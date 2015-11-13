package eu.pepot.eu.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// LESSONS:
// - Use array types that might end up being primitive light-weight JVM types
// - Avoid conversions as much as possible
// - Avoid groupBy! Use better reduceByKey (to discard elements as soon as possible, decreases execution time by half!!!)
// - Think about repartitioning to avoid overhead of data transfer when having too many partitions on distant executors (reduces 1/7 the time)
// - Think better about coalescing with the amount of workers, it can only decrease the amount of partitions with less data transfering than a full shuffle
//   and speds up processing (decreases execution time to one third of the original one!!!!)
// - Coalescing (4 threads in local master):
//    NO  ->  47s
//     4  ->  26s
//     8  ->  26s
//    16  ->  28s
//    32  ->  31s
//    64  ->  38s
//   128  ->  46s


object Example {

  type CsvLine = Array[String]

  def main(args: Array[String]) {

    val inputDirectory = "localhdfs/input"
    val outputDirectory = "localhdfs/output"


    implicit val sc = new SparkContext(new SparkConf())

    val rawLines: RDD[String] = sc.textFile(inputDirectory)

    val splitLines = rawLines.map(transformLineToCsvLine)

    // This algorithm keeps only the latest lines
    //val csvLinesFlagged = csvLines.keyBy(getKey).reduceByKey(getLatestCsvLine).values.map(csv => ("Y" ++ csv).mkString(","))
    //csvLinesFlagged.saveAsTextFile(outputDirectory)

    // This algorithm keeps all the lines
    val allReducedEventsFlagged = splitLines.map(getKeyAndEventNumber)
      .groupBy(getKey)
      .flatMap { case (key, csvLinesWithSameKey) =>
        val newestEventWithinTheGroup = csvLinesWithSameKey.map(getEventNumber).max
        val flaggedCsvLines = csvLinesWithSameKey.map { csvLine =>
          if (getEventNumber(csvLine) == newestEventWithinTheGroup) {
            (getKey(csvLine) + "," + getEventNumber(csvLine), "Y")
          } else {
            (getKey(csvLine) + "," + getEventNumber(csvLine), "N")
          }
        }
        flaggedCsvLines
      }
      .coalesce(2)
      .cache()

    val allCsvLinesFlagged = splitLines.keyBy(csvLine => getKey(csvLine) + "," + getEventNumber(csvLine)).join(allReducedEventsFlagged).map { case (id, (a, b)) =>
      a :+ b
    }
    allCsvLinesFlagged.map(_.mkString(",")).saveAsTextFile(outputDirectory)


  }

  def transformLineToCsvLine(lineString: String): CsvLine = lineString.split(",")

  def getKey(csv: Array[String]): String = csv(0)

  def getEventNumber(csv: CsvLine): String = csv(1)

  def getLatestCsvLine(csvLine1: CsvLine, csvLine2: CsvLine): CsvLine = if (getEventNumber(csvLine1) > getEventNumber(csvLine2)) csvLine1 else csvLine2

  def getKeyAndEventNumber(csv: CsvLine): CsvLine = csv.slice(0, 2)

}

