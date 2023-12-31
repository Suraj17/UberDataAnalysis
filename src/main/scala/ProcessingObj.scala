import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.net.URL

object ProcessingObj {

  def main(args:Array[String]):Unit = {

    val conf = new SparkConf().
      setAppName("first").
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

    // payment methods used different rides
    df.groupBy(col("Payment_type")).count().show(10,false)

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

    val paymentLookupDf = paymentTypeLookupTplArr2.toSeq.toDF(lkpPaymentCols:_*)

    paymentLookupDf.show(10,false)

  }

}
