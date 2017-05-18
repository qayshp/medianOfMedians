import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class MedianOfMediansCalculator {

  def calculateMedianOfMediansForFile(hdfsFilePath: String, sc: SparkContext) =
    calculateMedianOfMedians(sortAndNumberMedians(calculateMediansPerLine(readFileOfIntegers(hdfsFilePath, sc))))

  def readFileOfIntegers(hdfsFilePath: String, sc: SparkContext): RDD[Array[Int]] = {
    sc.textFile(hdfsFilePath)
      .map(line => line.split("\\D+"))
      .map(lineParts => lineParts.map(number => number.toInt)
        .sorted)
  }

  def calculateMediansPerLine(integerArrayRdd: RDD[Array[Int]]): RDD[Double] = {
    integerArrayRdd.map { lineInts =>
      if (lineInts.length % 2 == 0)
        (lineInts(lineInts.length / 2) + lineInts((lineInts.length / 2) + 1)) / 2.0
      else
        lineInts((lineInts.length / 2) + 1)
    }
  }

  def sortAndNumberMedians(lineMedians: RDD[Double]): RDD[(Long, Double)] = {
    lineMedians
      .sortBy(identity)
      .zipWithIndex
      .keyBy { case (_, index) => index }
      .mapValues { case (value, _) => value }
  }

  def calculateMedianOfMedians(sortedAndNumberedMedians: RDD[(Long, Double)]): Double = {
    if (sortedAndNumberedMedians.count() % 2 == 0)
      sortedAndNumberedMedians.lookup((sortedAndNumberedMedians.count / 2) + 1).head + sortedAndNumberedMedians.lookup(sortedAndNumberedMedians.count / 2).head / 2.0
    else
      sortedAndNumberedMedians.lookup((sortedAndNumberedMedians.count / 2) + 1).head
  }
}