// Databricks notebook source
// MAGIC %md
// MAGIC # Virtual CO2 Devices
// MAGIC 
// MAGIC ## Background
// MAGIC Especially during the winter months of the pandemic, we find ourselves spending more time in indoor environments. Researchers have advised that white self-isolating, especially during the winter months, to monitor indoor CO2 levels (especially in public spaces) because [high CO2 levels can result in higher COVID transmission](https://www.cambridge.org/core/journals/flow/article/monitoring-carbon-dioxide-to-quantify-the-risk-of-indoor-airborne-transmission-of-covid19/245A8FE68DD9C07655B9F25BECE967D2). In addition aiding the infection vector, [high CO2 levels can cause other health problems](https://www.nature.com/articles/s41893-019-0323-1):
// MAGIC 
// MAGIC | CO2 ppm | notes |
// MAGIC | --- | --- |
// MAGIC | 250-350 | background (normal) outdoor air level |
// MAGIC | 350-1000 | typical level found in occupied spaces with good air exchange |
// MAGIC | 1000-2000 | level associated with complaints of drowsiness and poor air | 
// MAGIC | 2000-5000 | level associated with headaches, sleepiness, and stagnant, stale, stuffy air; poor concentration, loss of attention, increased heart rate and slight nausea may also be present. |
// MAGIC | >5000 | This indicates unusual air conditions where high levels of other gases also could be present. Toxicity or oxygen deprivation could occur. This is the permissible exposure limit for daily workplace exposures. |
// MAGIC | >40000 | This level is immediately harmful due to oxygen deprivation. | 
// MAGIC 
// MAGIC 
// MAGIC Naturally, the idea of suffocating humans bothers you, but luckily, your startup sells CO2 sensors to various establishments (schools, restaurants, offices, shops, homes) and notifies them if the daily levels reach above 1000 and offer recommendations to lower those numbers to an acceptable level. 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Mocking Data
// MAGIC As a result of a networking problem, the data from the devices haven't arrived. The fix won't be in until later this week, but you are determined to get started on data cleaning regardless. You already know that each device streams data every 5 minutes in the following shape:
// MAGIC 
// MAGIC | field | type | description |
// MAGIC | --- | --- | --- |
// MAGIC | interval | integer | The index of 5-minute interval that this data point pertains to. There are 12 five-minute intervals in an hour and 24 hours in a day, resulting in 12*24 five-minute intervals |
// MAGIC | co2_ppm | double | The CO2 (ppm) reading of the 5-minute interval |
// MAGIC | device | integer | An identifier of the device. The actual device data lives elsewhere because of GDPR reasons. |
// MAGIC 
// MAGIC ### Instructions
// MAGIC Create some mock data containing:
// MAGIC * `12*24` five-minute intervals (labeled 1 -> 12*24)
// MAGIC * co2_ppm values according to the normal distribution values (with values reaching a peak in the middle of the day, simulating CO2 levels in a building with closed windows in the deep of winter) for the distribution (mean, stdDev) of the `interval` column
// MAGIC * the above for 10 devices (numbered 1 -> 10)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise: Create a DataFrame
// MAGIC In the following code block, we create a DataFrame that holds all five minute intervals in a single day. Reminder: there are 12 five-minute intervals in an hour and 24 hours in a day. That's `12*24` five-minute intervals in a day. Please run the code block.

// COMMAND ----------

import org.apache.spark.sql.DataFrame

val fiveMinIntervalsInADay = 12*24
val df = (1 to fiveMinIntervalsInADay)
  .toSeq.toDF("interval")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise: Create a Function
// MAGIC Create a function that takes an interval and outputs the value for that interval according to the the Normal Distribution function line.
// MAGIC 
// MAGIC Some resources that will help you:
// MAGIC * [Definition of Normal Distribution](https://en.wikipedia.org/wiki/Normal_distribution)

// COMMAND ----------

import org.apache.commons.math3.util.FastMath.{PI, exp, pow, sqrt}

/// YOUR CODE BELOW

def normalDistribution(mean: Double, stdDev: Double, x: Int): Double = {
  // one line
  1/(stdDev*sqrt(2*PI))*(exp(-0.5 * pow((x-mean)/(stdDev), 2)))
}

// COMMAND ----------

// MAGIC %md
// MAGIC Run the tests to make sure you've implemented the correct normalDistribution function.

// COMMAND ----------

import org.apache.commons.math3.stat.descriptive.SummaryStatistics

def testNormalDistributionFunction(): Unit = {
  val inputIntervals = (1 to 10).toSeq
  val stats = new SummaryStatistics
  for (i <- inputIntervals) stats.addValue(i)
  val result = inputIntervals.map(x => normalDistribution(stats.getMean(), stats.getStandardDeviation(), x)).toSeq
  println(stats)

  val expected = Seq(
    0.04366227396405137,
    0.0675486493639003,
    0.09370211835810706,
    0.11654793120530371,
    0.12998168086968445,
    0.12998168086968445,
    0.11654793120530371,
    0.09370211835810706, 
    0.0675486493639003, 
    0.04366227396405137
  )
  assert(result == expected)
  println("All tests pass.")
}

testNormalDistributionFunction()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise: Create a new column with values from your new function
// MAGIC It is possible to create new columns using the Spark API and functions that you define. For example:

// COMMAND ----------

import org.apache.spark.sql.functions.{col, lit}

val exampleDf = (1 to 10).toSeq.toDF("interval")
exampleDf.show()

exampleDf.withColumn("usingMath", col("interval")*10)
  .show()

exampleDf.withColumn("usingLiterals", col("interval") + lit(5)).show()

// COMMAND ----------

// MAGIC %md
// MAGIC You might also try to use a custom Scala function, but it fails dramatically when trying to create a new column. Why does it fail?

// COMMAND ----------

import org.apache.spark.sql.functions.{mean}

def exampleTransformationFunction(interval: Int): Int = {
  interval*2
}

// COMMAND ----------

exampleDf.withColumn("usingSparkFunctionMean", exampleTransformationFunction(col("interval")))

// COMMAND ----------

// MAGIC %md
// MAGIC In Spark, to use custom functions, you'll need to convert them to [UDFs (user-defined functions)](https://sparkbyexamples.com/spark/spark-sql-udf/).

// COMMAND ----------

import org.apache.spark.sql.functions.{udf}
def exampleTransformationFunctionUdf = udf((interval : Int) => exampleTransformationFunction(interval))
// NOTE: Shorthand: def exampleTransformationFunctionUdf = udf(exampleTransformationFunction(_))

exampleDf.withColumn("usingCustomFunction", exampleTransformationFunctionUdf(col("interval"))).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Now that you have a fancy new function (normalDistribution) that can calculate values which are outputted by the normal distribution function, create a new column called `co2_ppm_ndist` using the function you just created (normalDistribution) and [withColumn](https://sparkbyexamples.com/spark/spark-dataframe-withcolumn/).
// MAGIC 
// MAGIC **HINT:** In order to handle multiple parameters, you'll have to use [Currying](https://stackoverflow.com/questions/35546576/how-can-i-pass-extra-parameters-to-udfs-in-spark-sql)

// COMMAND ----------

import org.apache.spark.sql.functions.{col, lit, mean, randn, stddev, udf}


/// YOUR CODE BELOW
// Fill in ???
def normalDistributionUdf(mean: Double, stdDev: Double) = udf(( x: Int) => normalDistribution(mean, stdDev, x))

def createNewColumnWithFunction(): DataFrame => DataFrame =
  df =>
    // Fill in ???. 
    // Hint: how might you calculate the mean and standard deviation of the interval column in-line?
    // (Use `mean` and `stddev` functions from the Spark API, not the `SummaryStatistics` library)
    df.withColumn("co2_ppm_ndist", normalDistributionUdf(
      df.select(mean(col("interval"))).first().getDouble(0),
      df.select(stddev(col("interval"))).first().getDouble(0),
    )(col("interval")))

df.transform(createNewColumnWithFunction()).show()


// COMMAND ----------

// TESTING MODULE. DO NOT MODIFY.
def testNormalDistributionFunction(): Unit = {
  val testDf = (1 to 10).toSeq.toDF("interval")
  
  val expected = Seq(
    (1, 0.04366227396405137),
    (2, 0.0675486493639003),
    (3, 0.09370211835810706),
    (4, 0.11654793120530371),
    (5, 0.12998168086968445),
    (6, 0.12998168086968445),
    (7, 0.11654793120530371),
    (8, 0.09370211835810706), 
    (9, 0.0675486493639003), 
    (10, 0.04366227396405137)
  ).toDF("interval", "co2_ppm_ndist")
  expected.show()
  
  val mean = 5.5
  val stdDev = 3.0276503540974917
  
  val result = testDf.transform(createNewColumnWithFunction())
  val diff = result.except(expected)
  assert(diff.count() == 0)
  assert(result.columns.toSeq == Seq("interval", "co2_ppm_ndist"))
  println("All tests pass.")
}

testNormalDistributionFunction()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise: Scaling (part 1)
// MAGIC Now that you have been able to apply your function to create mock co2_ppm values along a normal distribution, you realise that something looks strange:
// MAGIC ```
// MAGIC +--------+--------------------+
// MAGIC |interval|       co2_ppm_ndist|
// MAGIC +--------+--------------------+
// MAGIC |       1|0.001085596275692...|
// MAGIC |       2|0.001108210408036...|
// MAGIC |       3|0.001131132523813...|
// MAGIC |       4|0.001154362315841...|
// MAGIC |       5|0.001177899336174...|
// MAGIC |       6|0.001201742993472622|
// MAGIC |       7|0.001225892550421...|
// MAGIC |       8|0.001250347121208...|
// MAGIC |       9|0.001275105669056...|
// MAGIC |      10|0.001300167003812759|
// MAGIC +--------+--------------------+
// MAGIC ```
// MAGIC Co2 PPM measurements are typically about 5x the scale. Create a new column called `co2_ppm_ndist_x5` to display alongside the existing `co2_ppm_ndist` column.
// MAGIC 
// MAGIC **NOTE:** typically, you might implement this calculation in-line but we would like to display any conversions as an exercise.

// COMMAND ----------

def scaleDistBy(x: Int): DataFrame => DataFrame =
  df =>
    // Fill in ???. 
    // HINT1: Make this flexible for other powers of 10
    // HINT2: How do you do string interpolation in Scala?
    df.withColumn(s"co2_ppm_ndist_x$x", col("co2_ppm_ndist")*Math.pow(10,x))

df
  .transform(createNewColumnWithFunction())
  .transform(scale_dist_by(5))
  .show(10)


// COMMAND ----------

// TESTING MODULE. DO NOT MODIFY.
def testScaleBy(): Unit = {
  val testDf = Seq(
    (1, 0.04366227396405137),
    (2, 0.0675486493639003),
    (3, 0.09370211835810706),
    (4, 0.11654793120530371),
    (5, 0.12998168086968445),
    (6, 0.12998168086968445),
    (7, 0.11654793120530371),
    (8, 0.09370211835810706), 
    (9, 0.0675486493639003), 
    (10, 0.04366227396405137)
  ).toDF("interval", "co2_ppm_ndist")
  
  val expected = Seq(
    (1, 0.04366227396405137, 4366.2273964051365),
    (2, 0.0675486493639003, 6754.86493639003),
    (3, 0.09370211835810706, 9370.211835810705),
    (4, 0.11654793120530371, 11654.793120530372),
    (5, 0.12998168086968445, 12998.168086968446),
    (6, 0.12998168086968445, 12998.168086968446),
    (7, 0.11654793120530371, 11654.793120530372),
    (8, 0.09370211835810706, 9370.211835810705), 
    (9, 0.0675486493639003, 6754.86493639003), 
    (10, 0.04366227396405137, 4366.2273964051365)
  ).toDF("interval", "co2_ppm_ndist", "co2_ppm_ndist_x5")
  
  val result = testDf.transform(scaleDistBy(5))
  result.show()
  val diff = result.except(expected)
  assert(diff.count() == 0)
  assert(result.columns.toSeq == Seq("interval", "co2_ppm_ndist", "co2_ppm_ndist_x5"))
  println("All tests pass.")
}

testScaleBy()

// COMMAND ----------

// MAGIC %md
// MAGIC Now, let's put it all together so far... (hint: view the plot)

// COMMAND ----------

display(df
  .transform(createNewColumnWithFunction())
  .transform(scaleDistBy(5)))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise: Scaling (part 2)
// MAGIC You might notice that we wanted to notify users when CO2 PPM gets to 1000 and 1800. Our current mock data does not reach those values. Let's scale our data in the `co2_ppm_ndist_x5` column once more by a factor of 3.8 and store that in a new column called `co2_ppm_3_8`.

// COMMAND ----------

/// YOUR CODE HERE
def scaleBy38(): DataFrame => DataFrame =
  df =>
    // Fill in ???. 
    df.withColumn("co2_ppm_3_8", col("co2_ppm_ndist_x5") * 3.8)


// COMMAND ----------

// TESTING MODULE. DO NOT MODIFY.
def testScaleBy38(): Unit = {
  val testDf = Seq(
    (1, 0.04366227396405137, 4366.2273964051365),
    (2, 0.0675486493639003, 6754.86493639003),
    (3, 0.09370211835810706, 9370.211835810705),
    (4, 0.11654793120530371, 11654.793120530372),
    (5, 0.12998168086968445, 12998.168086968446),
    (6, 0.12998168086968445, 12998.168086968446),
    (7, 0.11654793120530371, 11654.793120530372),
    (8, 0.09370211835810706, 9370.211835810705), 
    (9, 0.0675486493639003, 6754.86493639003), 
    (10, 0.04366227396405137, 4366.2273964051365)
  ).toDF("interval", "co2_ppm_ndist", "co2_ppm_ndist_x5")
  
  val expected = Seq(
    (1, 0.04366227396405137, 4366.2273964051365, 16591.66410633952),
    (2, 0.0675486493639003, 6754.86493639003, 25668.486758282113),
    (3, 0.09370211835810706, 9370.211835810705, 35606.80497608068),
    (4, 0.11654793120530371, 11654.793120530372, 44288.21385801541),
    (5, 0.12998168086968445, 12998.168086968446, 49393.03873048009),
    (6, 0.12998168086968445, 12998.168086968446, 49393.03873048009),
    (7, 0.11654793120530371, 11654.793120530372, 44288.21385801541),
    (8, 0.09370211835810706, 9370.211835810705, 35606.80497608068), 
    (9, 0.0675486493639003, 6754.86493639003, 25668.486758282113), 
    (10, 0.04366227396405137, 4366.2273964051365, 16591.66410633952)
  ).toDF("interval", "co2_ppm_ndist", "co2_ppm_ndist_x5", "co2_ppm_3_8")
  
  val result = testDf.transform(scaleBy38())
  result.show()
  val diff = result.except(expected)
  assert(diff.count() == 0)
  assert(result.columns.toSeq == Seq("interval", "co2_ppm_ndist", "co2_ppm_ndist_x5", "co2_ppm_3_8"))
  println("All tests pass.")
}

testScaleBy38()

// COMMAND ----------

// MAGIC %md
// MAGIC Let's put it together!

// COMMAND ----------

val scaling38Df = df
  .transform(createNewColumnWithFunction())
  .transform(scaleDistBy(5))
  .transform(scaleBy38())
  
scaling38Df.show()

display(scaling38Df) 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise: Adding some NOISE
// MAGIC While we might be generating the same shape of data for every device, every device is different. In this exercise, we'll be adding some noise using the formula `randn(1)*10` in a new column called 'co2_ppm_noise'.
// MAGIC 
// MAGIC ### Resources
// MAGIC * [Spark SQL Function randn](https://spark.apache.org/docs/latest/api/sql/index.html#randn)

// COMMAND ----------

/// YOUR CODE HERE
def addNoise(): DataFrame => DataFrame =
  df =>
    // Fill in ???. 
    df.withColumn("co2_ppm_noise", col("co2_ppm_3_8") + randn(1)*10)

// COMMAND ----------

// TESTING MODULE. DO NOT MODIFY.
def testNoise(): Unit = {
  val testDf = Seq(
    (1, 16591.66410633952),
    (2, 25668.486758282113),
    (3, 35606.80497608068),
    (4, 44288.21385801541),
    (5, 49393.03873048009),
    (6, 49393.03873048009),
    (7, 44288.21385801541),
    (8, 35606.80497608068), 
    (9, 25668.486758282113), 
    (10, 16591.66410633952)
  ).toDF("interval", "co2_ppm_3_8")
  
  val expected = Seq(
    (1, 16591.66410633952, 16608.509717593963),
    (2, 25668.486758282113, 25680.76282837649),
    (3, 35606.80497608068, 35614.165608987576),
    (4, 44288.21385801541, 44292.72211550427),
    (5, 49393.03873048009, 49389.04597101324),
    (6, 49393.03873048009, 49387.297109300096),
    (7, 44288.21385801541, 44272.33939452365),
    (8, 35606.80497608068, 35621.57160584409), 
    (9, 25668.486758282113, 25670.13828771209), 
    (10, 16591.66410633952, 16580.24487593498)
  ).toDF("interval", "co2_ppm_3_8", "co2_ppm_noise")
  
  val result = testDf.transform(addNoise())
  result.show()
  
  val diff = result.except(expected)
  assert(diff.count() == 0)
  assert(result.columns.toSeq == Seq("interval", "co2_ppm_3_8", "co2_ppm_noise"))
  println("All tests pass.")
}

testNoise()

// COMMAND ----------

// MAGIC %md
// MAGIC Let's put it together!

// COMMAND ----------

val withNoiseDf = df
  .transform(createNewColumnWithFunction())
  .transform(scaleDistBy(5))
  .transform(scaleBy38())
  .transform(addNoise())

withNoiseDf.show()

display(withNoiseDf) 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise: Cleaning up
// MAGIC Remember that we wanted to have the following schema:
// MAGIC 
// MAGIC 
// MAGIC | field | type | description |
// MAGIC | --- | --- | --- |
// MAGIC | interval | integer | The index of 5-minute interval that this data point pertains to. There are 12 five-minute intervals in an hour and 24 hours in a day, resulting in 12*24 five-minute intervals |
// MAGIC | co2_ppm | double | The CO2 (ppm) reading of the 5-minute interval |
// MAGIC | device | integer | An identifier of the device. The actual device data lives elsewhere because of GDPR reasons. |
// MAGIC 
// MAGIC We have so far calculated some mock data for our `co2_ppm`. The currently schema after all of the transformations looks like:

// COMMAND ----------

df
  .transform(createNewColumnWithFunction())
  .transform(scaleDistBy(5))
  .transform(scaleBy38())
  .transform(addNoise())
  .printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Return only the `interval` and `co2_ppm` (renamed from `co2_ppm_noise`) column.
// MAGIC 
// MAGIC ### References
// MAGIC * [Spark Select](https://sparkbyexamples.com/spark/spark-select-vs-selectexpr-with-examples/)

// COMMAND ----------

/// YOUR CODE HERE
// Fill in the ???
def cleanup(): DataFrame => DataFrame =
  df =>df
    .transform(createNewColumnWithFunction())
    .transform(scaleDistBy(5))
    .transform(scaleBy38())
    .transform(addNoise())
    .select(
      col("interval"),
      col("co2_ppm_noise").as("co2_ppm")
    )

selectedDf.printSchema()

// COMMAND ----------

// TESTING MODULE. DO NOT MODIFY.
def testCleanup(): Unit = {
  val testDf = Seq(
    (1, 16608.509717593963),
    (2, 25680.76282837649),
    (3, 35614.165608987576),
    (4, 44292.72211550427),
    (5, 49389.04597101324),
    (6, 49387.297109300096),
    (7, 44272.33939452365),
    (8, 35621.57160584409), 
    (9, 25670.13828771209), 
    (10, 16580.24487593498)
  ).toDF("interval", "co2_ppm_noise")
  
  val expected =  Seq(
    (1, 16608.509717593963),
    (2, 25680.76282837649),
    (3, 35614.165608987576),
    (4, 44292.72211550427),
    (5, 49389.04597101324),
    (6, 49387.297109300096),
    (7, 44272.33939452365),
    (8, 35621.57160584409), 
    (9, 25670.13828771209), 
    (10, 16580.24487593498)
  ).toDF("interval", "co2_ppm")
  
  val result = testDf.transform(cleanup())
  val diff = result.except(expected)

  assert(diff.count() == 0)
  assert(result.columns.toSeq == Seq("interval", "co2_ppm"))
  println("All tests pass.")
}

testCleanup()

// COMMAND ----------

// MAGIC %md
// MAGIC Putting it all together:

// COMMAND ----------

df
  .transform(createNewColumnWithFunction())
  .transform(scaleDistBy(5))
  .transform(scaleBy38())
  .transform(addNoise())
  .transform(cleanup())
  .show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise: Create data for multiple devices
// MAGIC Now that we have mock co2 data for a single day, we'd like to create this data for multiple devices and combine them all into a single dataframe.

// COMMAND ----------

/// YOUR CODE HERE
// Replace ???
def generateSingleSample(initialDf: DataFrame, deviceNumber: Int): DataFrame = {
  initialDf
    .transform(createNewColumnWithFunction())
    .transform(scaleDistBy(5))
    .transform(scaleBy38())
    .transform(addNoise())
    .transform(cleanup())
    .withColumn("device", lit(deviceNumber))
}

val deviceNumbers = (1 to 10).toSeq

// Replace ???
def generateDataForAllDevices(initialDf: DataFrame, deviceNumbers: Seq[Int]): Seq[DataFrame] = {
  deviceNumbers.map(x => generateSingleSample(initialDf, x)).toSeq
}

def combineIntoOneDataFrame(devices: Seq[DataFrame]): DataFrame = {
  // Replace ??? with one Line
  devices.reduce(_ union _)
}

combineIntoOneDataFrame(generateDataForAllDevices(df, deviceNumbers)).show()

// COMMAND ----------

// TESTING MODULE. DO NOT MODIFY.
def testCleanup(): Unit = {
  val testDf = (1 to 3).toSeq.toDF("interval")
  
  val expected =  Seq(
    (1,91965.72092852893,1),
    (2,151610.3426226388,1),
    (3,91956.23595018136,1),
    (1,91965.72092852893,2),
    (2,151610.3426226388,2),
    (3,91956.23595018136,2)
  ).toDF("interval", "co2_ppm", "device")
  
  val result = combineIntoOneDataFrame(generateDataForAllDevices(testDf, Seq(1, 2)))
  result.show()
  val diff = result.except(expected)
  assert(diff.count() == 0)
  assert(result.columns.toSeq == Seq("interval", "co2_ppm", "device"))
  println("All tests pass.")
}

testCleanup()

// COMMAND ----------

display(combineIntoOneDataFrame(generateDataForAllDevices(df, deviceNumbers)))
