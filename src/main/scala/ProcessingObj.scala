import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.net.URL

object ProcessingObj {

  def main(args:Array[String]):Unit = {

    val conf = new SparkConf().
      setAppName("UberDataAnalysis").
      setMaster("local[*]").
      set("spark.driver.host", "localhost").
      set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

//    import spark.implicits._

    val rdd: RDD[Int] = sc.parallelize(Array(5,10,13))

    println(rdd.reduce(_ + _))

    val resource: URL = getClass.getResource("uber_data.csv")

    println("File :- " + resource.getFile);

    val df = spark.read.format("csv").
      option("header","true").
      option("inferSchema","true").
      load(s"""file:///${resource.getFile.replace("%20"," ")}""")

    df.show(10,false)

    df.printSchema()

    println(df.count())

    // relationship between total_amount and fare_amount
    // this is just some general data wrangling to understand the raw data better

    df.select(count("*").as("total_rows"),
      sum(when(col("Total_amount") >= col("Fare_amount"), lit(1))
        .otherwise(lit(0))).as("cnt_total_amt"),
      sum(when(col("Total_amount") < col("Fare_amount"), lit(1))
        .otherwise(lit(0))).as("cnt_fare_amt")
    ).show(100, false)

    // payment methods used for different rides
    val ridePaymentMethodCountsDf = df.groupBy(col("Payment_type")).count()

    ridePaymentMethodCountsDf.show(10,false)

    val paymentTypeLookupsString =
      """
        |1 = Credit card
        |2 = Cash
        |3 = No charge
        |4 = Dispute
        |5 = Unknown
        |6 = Voided trip
        |""".stripMargin

    val paymentTypeLookupsStr2 = paymentTypeLookupsString.
      replaceAll(" ","")
//      .replaceAll("\n",",")
//      .split("=")

    println(paymentTypeLookupsStr2)

    val paymentTypeLookupsArrStr1: Array[String] = paymentTypeLookupsStr2.split("\r\n")
      .filter(x => x.nonEmpty)

    paymentTypeLookupsArrStr1.foreach(x => println(s"[ ${x} ]"))

    println

    println(s"""paymentTypeLookupsArrStr1 -> ${paymentTypeLookupsArrStr1.length} ${paymentTypeLookupsArrStr1.mkString("|")}""")

    println

    val paymentTypeLookupTplArr = paymentTypeLookupsArrStr1.map(x => {
      val tpl: Array[String] = x.split("=")
      println(s"${tpl.length} ${tpl.mkString("|")}")
//      Tuple2(tpl(0),tpl(1))
      tpl.mkString(",")
    })

    println

    paymentTypeLookupTplArr.foreach(x => println(x))

    println

    val paymentTypeLookupTplArr2: Array[(String, String)] = paymentTypeLookupsArrStr1.map(x => {
      val tpl: Array[String] = x.split("=")
      Tuple2(tpl(0),tpl(1))
    })

    paymentTypeLookupTplArr2.foreach(println)

    import spark.implicits._

    val lkpPaymentCols = Seq("paymentTypeId","paymentTypeName")

    val allPaymentTypesDf = paymentTypeLookupTplArr2.toSeq.toDF(lkpPaymentCols:_*)

    allPaymentTypesDf.show(10,false)

    // Q1. Print the name of payment method used and the payment method count.

    val paymentMethodsAndCounts = allPaymentTypesDf.join(ridePaymentMethodCountsDf,
      allPaymentTypesDf("paymentTypeId")===ridePaymentMethodCountsDf("Payment_type"),
      "left")

    paymentMethodsAndCounts.show(10,false)

    val paymentMethodsAndCountsFinal = paymentMethodsAndCounts
      .drop(col("Payment_type"))
      .withColumnRenamed("count","numberOfTimesPaymentMethodUsed")

    paymentMethodsAndCountsFinal.show(10,false)

    // Q2. Want to know which payment mode is more frequent choice of riders

    val mostNumOfTimesUsedPMC = paymentMethodsAndCountsFinal.
      select(max("numberOfTimesPaymentMethodUsed").as("maxCntPM"))
      .first().getAs[Long]("maxCntPM")


    val detailsOfPMUsedMostNumOfTimes = paymentMethodsAndCountsFinal.
      filter(col("numberOfTimesPaymentMethodUsed")===mostNumOfTimesUsedPMC)

    detailsOfPMUsedMostNumOfTimes.show(5,false)

    //Q3. calculate total Revenue, record count, Avg trip distance, Avg Fare amount, Avg Trip Amount

    val generalStatsDf = df.agg(sum(col("total_amount")).as("revenue"),
      count(col("*")).as("recordCount"),
      avg(col("Trip_distance")).as("AvgTripDistance"),
      avg(col("Fare_amount")).as("AvgFareAmount"),
      avg(col("Tip_amount")).as("AvgTipAmount")
    )

    generalStatsDf.show(10,false)

    //Q3. Sub Tasks
    // 1. Round all decimal places to 3 digits.
    // 2. Amount Cols should be appended with USD and Distance cols should be appended with Miles

    val generalStatsFormattedDf = generalStatsDf
      .withColumn("revenue", concat_ws(" ",round(col("revenue"),3),lit("USD")))
      .withColumn("AvgTripDistance", concat_ws(" ",round(col("AvgTripDistance"),3), lit("Miles")))
      .withColumn("AvgFareAmount", concat_ws(" ",round(col("AvgFareAmount"),3), lit("USD")))
      .withColumn("AvgTipAmount", concat_ws(" ",round(col("AvgTipAmount"),3), lit("USD")))

    generalStatsFormattedDf.show(10,false)

  }

}
