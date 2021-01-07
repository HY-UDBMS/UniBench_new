import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import Product.CreateProduct
import breeze.stats.distributions.Poisson
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, LongType, StructField}

/**
 * Created by chzhang on 19/06/2017.
 */
object Order_Review_Invoice {

  def Create(spark: SparkSession, filename: String, DateBound: Int) = {

    // Configure the serialize path
    val path_order = spark.conf.get("order")
    val path_feedback = spark.conf.get("feedback")
    val path_invoice = spark.conf.get("invoice")
    val path_rating = spark.conf.get("rating")

    // helper function in the following fold block
    // Sample the id according to the interest table
    def takeSample(a: Array[String], mean: Int): ArrayBuffer[Array[String]] = {
      val NumberOfTransaction = a.length / mean
      var PurchaseTransaction = ArrayBuffer[Array[String]]()

      for (i <- 0 until NumberOfTransaction) {
        val size = Poisson(mean).sample(1)
        PurchaseTransaction += Array.fill(size(0))(a(Random.nextInt(a.length)))
      }
      PurchaseTransaction
    }

    // Divide the order of same person into sub-order
    def split[Any](xs: List[Any], n: Int): List[List[Any]] = {
      if (xs.isEmpty) Nil
      else (xs take n) :: split(xs drop n, n)
    }

    val GetRating = (s: String) => {
      val Reg = "(\\d)".r
      val matches = Reg.findFirstIn(s).get
      matches
    }

    val RandomReview = (s: String) => {
      val RegPattern = "\'(.*?)\'".r
      val matches = RegPattern.findAllIn(s).toList
      matches(Random.nextInt(matches.size))
    }

    // Compute the totalamount of single order
    val computeTotalamount: Seq[Double] => Double = (item: Seq[Double]) => BigDecimal(item.reduceLeft(_ + _)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

    val asin = StructField("asin", DataTypes.StringType)
    val title = StructField("title", DataTypes.StringType)
    val price = StructField("price", DataTypes.DoubleType)
    // add the brand field
    val brand = StructField("brand", DataTypes.StringType)
    val schema_order = """array<array<struct<productId:String,asin:String,title:String,price:Double,brand:String>>>"""

    val ProductDF = CreateProduct(spark)
    // Mapping the product id to asin
    val ProductforOrder = ProductDF.select("asin", "title", "price", "feedback").toDF()
    import spark.implicits._
    val IdMappingRDD = spark.sparkContext.textFile("src/main/resources/tagMatrix.txt").map(_.split(" ")).map(u => (u(1), u(3), u(0))).toDF("productId", "asin", "brand")
    val index = spark.sparkContext.textFile("src/main/resources/PopularSportsBrandByCountry.csv").map(_.split(",")).map(u => (u(0), u(1))).toDF("brand", "tag")

    val intermediateResult = IdMappingRDD.join(ProductforOrder, "asin").join(index, "brand")

    // Construct the order according to interest table
    val InterestByperson = spark.read.option("header", "true").option("delimiter", "|").csv(filename).toDF("PersonId", "productId")

    // Split the data into two parts, one part in which people have interests less than 5, while the other part is the rest of part
    /*    val minority=InterestByperson
          .groupBy("PersonID")
          .count()
            .filter($"count">3)

        val firstPart=InterestByperson.join(minority,"PersonID").repartition(1).drop("count")*/

    //firstPart.groupBy("PersonID").count().rdd.map(r=>r(0)).foreach(println)


    // Key-value for feedback
    val feedbacklist = InterestByperson.join(intermediateResult, "productId")
    val randomFeedback = udf(RandomReview)
    val feedback = feedbacklist.withColumn("feedback", randomFeedback(col("feedback")))
    //val feedbackwithoutdupli=feedback.dropDuplicates(Array("productId","feedback"))

    val extract = udf(GetRating)
    feedback.withColumn("rating", extract(col("feedback"))).select("PersonId", "productId", "rating").repartition(1).write.option("delimiter", "|").csv(path_rating)

    /*
    //Key-value for feedback
    feedback.createOrReplaceTempView("Feedback")
    spark.sqlContext
      .sql("SELECT asin,PersonId,feedback FROM Feedback")
      .sample(false, spark.conf.get("feedback_factor").toDouble)
      .repartition(1)
      .write
      .option("delimiter", "|")
      .option("header", "true")
      .csv(path_feedback)
*/
    val result = InterestByperson.join(intermediateResult, "productId").toDF("_1", "PersonId", "_0", "_2", "_3", "_4", "_6", "_5")

    val resultWithoutfeedback = result
      .select("PersonId", "_1", "_2", "_3", "_4", "_5")
      .groupBy($"PersonId")
      .agg(collect_set((struct("_1", "_2", "_3", "_4", "_5"))))
      .toDF("PersonId", "order")

    val orderid = udf(() => java.util.UUID.randomUUID.toString)

    val rand = new Random(DateBound)
    val randDateUdf = udf(() => (DateBound + rand.nextInt(7)) + "-" + (1 + rand.nextInt(11)).toString.reverse.padTo(2, "0").reverse.mkString + "-" + (1 + rand.nextInt(29)).toString.reverse.padTo(2, "0").reverse.mkString)
    val divide: (Seq[(String, String, String, Double, String)] => List[List[(String, String, String, Double, String)]]) = (arg: Seq[(String, String, String, Double, String)]) => {
      split(arg.toList, 5)
    }
    val subList = udf(divide)
    val flattened = resultWithoutfeedback
      .withColumn("Orders", subList(resultWithoutfeedback("order"))).drop(col("order"))
      .withColumn("OrderwithSchema", $"Orders".cast(schema_order))
      .withColumn("Orderline", explode(col("OrderwithSchema"))).drop(col("OrderwithSchema"))
      .withColumn("OrderId", orderid())
      .withColumn("OrderDate", randDateUdf()).drop("Orders")
      .withColumn("PersonId", $"PersonId".cast(LongType))

    flattened.createOrReplaceTempView("Orders")
    spark.sqlContext.udf.register("computeTotalamount", computeTotalamount(_: Seq[Double]))

    // JSON for order
    spark.sqlContext
      .sql("SELECT OrderId,PersonId,OrderDate,computeTotalamount(Orderline.price) AS TotalPrice,Orderline FROM Orders").repartition(1).write.json(path_order)

    // XML for invoice
    val Invoice = spark.sqlContext
      .sql("SELECT OrderId,PersonId,OrderDate,computeTotalamount(Orderline.price) AS TotalPrice,Orderline FROM Orders")
      .repartition(1).write.format("com.databricks.spark.xml").option("rootTag", "Invoices").option("rowTag", "Invoice.xml").save(path_invoice)

    //flattened

  }

}
