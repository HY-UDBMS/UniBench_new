import java.io.FileInputStream
import java.util.Properties
import scala.util.Random
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, rank, _}
import java.nio.file.Files
import java.nio.file.Paths
/**
 * Unibench2.0 with Relational, Graph, XML, Key-value and RDF data
 */
object Unibench2_0 {

  def Propagation_Purchase(spark: SparkSession) = {
    // Configure the path
    val Person_knows_Person = spark.conf.get("Person_knows_Person")
    val Person_has_interests = spark.conf.get("interest_table")
    val ratingMatrixPath = spark.conf.get("rating")
    //val rating_path = spark.sparkContext.wholeTextFiles(ratingMatrixPath).map { case (filename, content) => filename}
    val rating_path = spark.sparkContext.textFile(ratingMatrixPath + "/part-*")

    // Construct the hashmap based on friends
    def CreatefriendMap() = {

      // Construct the order according to interest table
      val KnowsGraph = spark.sparkContext.textFile(Person_knows_Person)
        .map(_.split("\\|"))
        .map(u => (u(0), Set(u(1))))
        .reduceByKey(_ ++ _).map {
        case (x, y) => (x, (x, y))
      }

      // Put the hashmap of person's friendship into IndexedRDD
      val IndexedRDDforKnows = IndexedRDD(KnowsGraph).cache()
      IndexedRDDforKnows
    }


    val ratingMatrix = spark.sparkContext.textFile(ratingMatrixPath)
      .map(_.split("\\|"))
      .map(u => Ratings(u(0), u(1), u(2).toInt)).cache()


    val majority = spark.sparkContext.textFile(Person_has_interests)
    val header = majority.first()
    val IndexedRDDforKnows = CreatefriendMap()

    val broadcastMap = spark.sparkContext.broadcast(IndexedRDDforKnows.collectAsMap())
    val broadcastRating = spark.sparkContext.broadcast(ratingMatrix.collect())

    var output: RDD[Unibench2_0.Ratings] = spark.sparkContext.emptyRDD

    val targetuser = majority.filter(row => row != header)
      .map(_.split("\\|"))
      .map(u => (u(0), Array(u(1))))
      .reduceByKey(_ ++ _)
      .filter(u => u._2.size < (50 * 5 / 4))
      .map(_._1)

    import spark.implicits._

    val friendSet = targetuser.map(broadcastMap.value.get(_))
      .filter(_.isDefined)
      .map(value => value.get)

    // Window definition
    val w = Window.partitionBy($"targetUser").orderBy(desc("avg(rating)"))

    // Fetch products of top-n rating with respect to each user
    val pro_purchase = friendSet.map {
      case (userid, set) =>
        val productset = broadcastRating.value
          .filter(x => set.contains(x.user_id))
        (userid, productset)
    }
      .filter(!_._2.isEmpty)
      .toDF("targetUser", "Ratings")
      .withColumn("repurchase", explode(col("Ratings"))).drop(col("Ratings"))
      .select("targetUser", "repurchase.product_id", "repurchase.rating")
      .groupBy("targetUser", "product_id").avg("rating")
      .withColumn("rank", rank.over(w)).where($"rank" <= 3)
      .select("targetUser", "product_id")
      .repartition(1)
      .write.option("header", "true").option("delimiter", "|").csv("NewInterestsTable")


    /*    spark.conf.set("order","Unibench/pro_purchase_order")
        spark.conf.set("feedback","Unibench/pro_purchase_feedback")
        spark.conf.set("invoice","Unibench/pro_purchase_invoice")
        spark.conf.set("rating","Unibench/pro_purchase_rating")*/

    //Order_Review_Invoice.Create(spark,"NewInterestsTable/part-*",2011)

  }

  def Train_model(spark: SparkSession) = {
    // Read data and train the model
    val OrderDF = DataPreparation.DataProcessing(spark, "Unibench/Purchase_order/part-*")
      .withColumn("Product", explode(col("Orderline"))).drop(col("Orderline"))
      .select("PersonId", "OrderDate", "Product.asin", "Product.price")

    // Link brand
    val RFMPath = spark.conf.get("BrandByProduct")

    val Brand2Product = spark.read.option("header", "false").option("delimiter", ",").csv(RFMPath).toDF("Brand_sign", "asin")
    val Order2Brand = OrderDF.join(Brand2Product, "asin")

    // Generate the Suffient statistic matrix for the CLVSC model
    Order2Brand.groupBy("Brand_sign", "PersonId", "OrderDate").sum("price")
      .repartition(1).write.csv("CLVSC")


    // estimate the parameter to CLVSV_parameters.txt

    val UnseenOrderDF = DataPreparation.DataProcessing(spark, "Unibench/pro_purchase_order/part-*")
      .withColumn("Product", explode(col("Orderline"))).drop(col("Orderline"))
      .select("PersonId", "OrderDate", "Product.asin", "Product.price")
    val BrandPath = spark.conf.get("BrandByProduct")
    val BrandByProduct = spark.read.option("header", "false").option("delimiter", ",").csv(BrandPath).toDF("Brand_sign", "asin")
    val OrderDFwithBrand = UnseenOrderDF.join(BrandByProduct, "asin")
      .groupBy("Brand_sign", "PersonId", "OrderDate").sum("price")
      .repartition(1).write.csv("repurchase")

  }

  def Re_Purchase(spark: SparkSession) = {
    def RandomSublist(lst: Seq[String]): Seq[String] = {
      Random.shuffle(lst).take(5)
    }

    val randomlist: Seq[String] => Seq[String] = (arg: Seq[String]) => {
      RandomSublist(arg)
    }
    val randomlst = udf(randomlist)

    // Construct the hashmap based on friends
    def CreateBrandToModelparMap() = {
      // Construct the order according to interest table

      val Modelpar = spark.sparkContext.textFile("CLVSV_parameters.txt")
        .map(_.split("\\s+"))
        .map(u => (u(0), Seq(u(1), u(2), u(3), u(4))))

      // Put the hashmap of person's friendship into IndexedRDD
      val IndexedRDDforKnows = IndexedRDD(Modelpar).cache()
      IndexedRDDforKnows
    }

    val parameters = CreateBrandToModelparMap()
    val broadcastModel = spark.sparkContext.broadcast(parameters.collectAsMap())

    def statisticByYear(lst: Seq[Int]): RFC = {
      val length = lst.distinct.length
      val max = lst.max - 2010
      RFC(length, max)
    }

    val divide: Seq[Int] => RFC = (arg: Seq[Int]) => {
      statisticByYear(arg)
    }
    val toInt = udf[Int, String](_.toInt)
    val subList = udf(divide)
    import spark.implicits._

    val data = spark.read.option("header", "false").option("delimiter", ",").csv("repurchase/part*").toDF("Brand", "cust", "date", "spend")
      .withColumn("date", substring(col("date"), 0, 4)).drop("spend")
      .withColumn("date", toInt(col("date")))
      .groupBy("Brand", "cust").agg(collect_list("date"))
      .withColumn("RF", subList(col("collect_list(date)")))
      .drop("collect_list(date)").select("Brand", "cust", "RF.frequency", "RF.recency").as[CLV].rdd


    // Window definition
    val w = Window.partitionBy($"Brand").orderBy(desc("score"))

    val CustomerValuebyBrand = data.map {
      x =>
        val y = broadcastModel.value.get(x.brand)
        val z = CLVSC.bgbb_Predict(y.get(0).toDouble, y.get(1).toDouble, y.get(2).toDouble, y.get(3).toDouble, x.frequency, x.recency, 7, 10)
        (x.brand, x.cust, z)
    }.filter(!_._3.isNaN)
      .toDF("Brand", "cust", "score")
      .withColumn("rank", rank.over(w)).where($"rank" <= 30)

    //Create the Brand-product mapper from SportsBrandProduct
    val Brand_has_products = spark.conf.get("BrandByProduct")

    val Brand2ProductMapper = spark.sparkContext.textFile(Brand_has_products)
      .map(_.split(","))
      .map(u => (u(0), Set(u(1))))
      .reduceByKey(_ ++ _).mapValues(_.toSeq).toDF("Brand", "plist")

    // Map the Brand to product list by join then random the plist
    val CustomerValuebyProduct = CustomerValuebyBrand.join(Brand2ProductMapper, "Brand")
      .withColumn("plist", randomlst(col("plist")))
      .withColumn("asin", explode(col("plist")))
      .select("cust", "asin")

    // Generate the NewInterstsTable for repurchase by join
    val IdMappingRDD = spark.sparkContext.textFile("src/main/resources/tagMatrix.txt").map(_.split(" ")).map(u => (u(1), u(3))).toDF("productId", "asin")
    val InterestTable = CustomerValuebyProduct.join(IdMappingRDD, "asin")
      .select("cust", "productId").sort("cust")
      .repartition(1)
      .write.option("header", "true").option("delimiter", "|").csv("NewInterestsTableforRepurchase")

    spark.conf.set("order", "Unibench/re_purchase_order")
    spark.conf.set("feedback", "Unibench/re_purchase_feedback")
    spark.conf.set("invoice", "Unibench/re_purchase_invoice")
    spark.conf.set("rating", "Unibench/re_purchase_rating")

    Order_Review_Invoice.Create(spark, "NewInterestsTableforRepurchase/part-*", 2017)
  }

  def main(args: Array[String]): Unit = {
    val params = new Properties
    try {
      params.load(new FileInputStream("unibench_params.ini"))
    } catch {
      case e: Exception =>
        println("Failed to parse params file: " + e.toString)
        return
    }

    val conf = new SparkConf().setAppName("Unibench").setMaster("local")
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //required by SANSA
      .config("spark.io.compression.codec", "snappy")
      .getOrCreate()

    { // get rid of DEBUG logs
      import ch.qos.logback.classic.{Level, Logger}
      import org.slf4j.LoggerFactory

      val rootLogger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]
      rootLogger.setLevel(Level.INFO)
    }

    spark.conf.set("feedback_factor", params.getProperty("feedback_factor", "1"))
    spark.conf.set("rdf_factor", params.getProperty("rdf_factor", "0"))
    spark.conf.set("interest_table", "Unibench/Graph_SocialNetwork/PersonHasInterest/part-*.csv")
    spark.conf.set("Person_knows_Person", "Unibench/Graph_SocialNetwork/part-*.csv")
    spark.conf.set("BrandByProduct", "src/main/resources/SportsBrandByProduct.csv")
    spark.conf.set("order", "Unibench/JSON_Order")
    spark.conf.set("parameter1", "Unibench/parameter1")
    spark.conf.set("feedback", "Unibench/CSV_Feedback")
    spark.conf.set("invoice", "Unibench/XML_Invoice")
    spark.conf.set("rating", "Unibench/KeyValue_Rating")
    spark.conf.set("rdf", "Unibench/RDF_Product")
    spark.conf.set("RFM", "RFM")

    SocialNetwork.graphGen(spark)

    Purchase(spark)

    // Product
    Product.WriteToDisk(spark, Product.CreateProduct(spark))

    // Vendor
    Vendor.GenerateVendor(spark)

    // Tag
    Utility.Copy("src/main/resources/","Unibench/Graph_SocialNetwork/Tag/tag.csv")

    RDFSimplified.Create(spark)

    // Rename the generated files
    Utility.reName("Unibench/CSV_Customer/","Unibench/CSV_Customer/person.csv")
    Utility.reName("Unibench/KeyValue_Rating/","Unibench/KeyValue_Rating/rating.csv")
    Utility.reName("Unibench/CSV_Vendor/","Unibench/CSV_Vendor/vendor.csv")
    Utility.reName("Unibench/CSV_Product/","Unibench/CSV_Product/product.csv")
    Utility.reName("Unibench/RDF_Product/","Unibench/RDF_Product/product.ttl")
    Utility.reName("Unibench/JSON_Order/","Unibench/JSON_Order/order.json")
    Utility.reName("Unibench/XML_Invoice/","Unibench/XML_Invoice/invoice.xml")
    Utility.reName("./Unibench/Graph_SocialNetwork/Post/","Unibench/Graph_SocialNetwork/Post/post.csv")
    Utility.reName("./Unibench/Graph_SocialNetwork/PersonKnowsPerson/","./Unibench/Graph_SocialNetwork/PersonKnowsPerson/person_knows_person.csv")
    Utility.reName("./Unibench/Graph_SocialNetwork/PersonHasPost/","./Unibench/Graph_SocialNetwork/PersonHasPost/person_has_post.csv")
    Utility.reName("./Unibench/Graph_SocialNetwork/PostHasTag/","./Unibench/Graph_SocialNetwork/PostHasTag/post_has_tag.csv")
    Utility.reName("./Unibench/Graph_SocialNetwork/PersonHasInterest/","./Unibench/Graph_SocialNetwork/PersonHasInterest/person_has_interest.csv")

    //Propagation_Purchase(spark)

    //Train_model(spark)

    //Re_Purchase(spark)

    /* Stop the sparkSession */
    spark.stop()
  }

  def Purchase(spark: SparkSession) = {
    val Person_has_interests = spark.conf.get("interest_table")
    Order_Review_Invoice.Create(spark, Person_has_interests, 2018)
  }

  case class Interest(personId: String, interestId: String)

  case class Ratings(user_id: String, product_id: String, rating: Int)

  case class Repurchase(user_id: String, product_set: Array[String])

  case class RFC(frequency: Int, recency: Int)

  case class CLV(brand: String, cust: String, frequency: Int, recency: Int)

}
