import java.io.FileInputStream
import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by chzhang on 25/06/2017.
 */
object RDF_gen {

  def post_has_tag_mapping(spark: SparkSession) = {
    // Load post_tag meta data
    // Mapping the product id to asin
    val Post_tag_dir = "../ldbc_snb_datagen/test_data/social_network/Post_hasTag_tag_*_0.csv"
    val Post_tag_DF = spark.read.option("header", "true").option("delimiter", "|").option("mode", "DROPMALFORMED").csv(Post_tag_dir).toDF("postId", "productId")
    import spark.implicits._
    val IdMappingRDD = spark.sparkContext.textFile("src/main/resources/tagMatrix.txt").map(_.split(" ")).map(u => (u(1), u(3))).toDF("productId", "asin")
    val new_post_tag = IdMappingRDD.join(Post_tag_DF, "productId").toDF()
    new_post_tag.select("postId", "asin").repartition(1).write.option("header", "true").option("delimiter", ",").csv("post_hasTag")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Unibench").setMaster("local")
    val spark = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    val params = new Properties
    try {
      params.load(new FileInputStream("unibench_params.ini"))
    } catch {
      case e: Exception =>
        println("Failed to parse params file: " + e.toString)
        return
    }
    // Generate the RDF data
    spark.conf.set("rdf_factor", params.getProperty("rdf_factor", "0"))
    spark.conf.set("rdf", "Unibench/Product_rdf")
    RDFSimplified.Create(spark)




    //val ProductDF = CreateProduct(spark)
    //print(ProductDF.show(5))
    // Mapping the product id to asin
    //val ProductforOrder = ProductDF.select("asin", "title", "price", "imgUrl")
    //import spark.implicits._
    //val IdMappingRDD = spark.sparkContext.textFile("src/main/resources/tagMatrix.txt").map(_.split(" ")).map(u => (u(1), u(3), u(0))).toDF("productId", "asin", "brand")

    //println(IdMappingRDD.count())

    //val productDF=ProductforOrder.join(IdMappingRDD,"asin").toDF().repartition(1).write.option("header","true").option("delimiter",",").csv("Product")
    // val index = spark.sparkContext.textFile("src/main/resources/PopularSportsBrandByCountry.csv").map(_.split(",")).map(u=>(u(0),u(1))).toDF("brand","tag")
    // val intermediateResult=IdMappingRDD.join(ProductforOrder,"asin").join(index,"brand").toDF().show(5)
    //post_has_tag_mapping(spark)
    // Construct the order according to interest table
    //val InterestByperson=spark.read.option("header","true").option("delimiter","|").csv("../ldbc_snb_datagen/test_data/social_network/person_hasInterest_tag_*_0.csv").toDF("PersonId","productId")

    //val result=InterestByperson.join(intermediateResult,"productId").toDF().show(5)

    //val cust=Customer.CreateCustomer(spark)

    //cust.write.option("header","true").option("delimiter",",").csv("customer")

    //val graph=Graph.CreateGraph(spark).toDF("from","to","CreateDate")
    //graph.repartition(1).write.option("header","true").option("delimiter",",").cxsv("graph")
    // Product
    //Product.WriteToDisk(spark,Product.CreateProduct(spark))
    //    import spark.implicits._
    //    val IdMappingRDD = spark.sparkContext.textFile("src/main/resources/tagMatrix2017.txt").map(_.split(" ")).map(u=>(u(1),u(3))).toDF("productId","asin")
    //    val Post_tag=spark.read.option("header","false").option("delimiter","|").csv("../ldbc_snb_datagen/test_data/social_network/post_hasTag_tag_*_0.csv").toDF("postId","productId")
    //    Post_tag.join(IdMappingRDD,"productId").select("postId","asin")
    //      .repartition(1)
    //      .write.option("header","true").option("delimiter","|").csv("post_hasTag")
    /*
        def statisticByYear(lst:List[Int]): RFC={
          val length=lst.distinct.length
          val max=lst.max
          RFC(length,max)
      }


    /*    import spark.implicits._
        val data=spark.sparkContext.textFile("src/main/resources/output_Anta_Sports")
          .map(_.split(","))
          .map(u=>(u(0),u(1) take(4)))
            .map(x=>(x._1,x._2.toInt-2002))
              .groupByKey().map(x=>(x._1,statisticByYear(x._2.toList)))
                .toDF()

        val summary=data.groupBy("_2")
          .count().select("_2.frequency","_2.recency","count").withColumn("Duraition",lit(7))
             .repartition(1).write.csv("SuffientStatisticMatrix")*/

          //.map{
         //case Row(r:Int,f:Int,count:Long) =>
         //  (r,f-2002,count.toInt)  // BG/BB Model
      //}.show()

          //.foreach{x=>x.foreach(println)}

        // Read data to create Brand-NumberofcorrelatedPost matrix
        val filename_post="../ldbc_snb_datagen/test_data/social_network/person_likes_post_*_0.csv"
        val filename_tag="../ldbc_snb_datagen/test_data/social_network/Post_hasTag_tag_*_0.csv"
        val filename_tagMatrix="src/main/resources/tagMatrix2017.txt"
        val filename_brand="src/main/resources/SportsBrandByProduct.csv"

        // val PersonLikesPost=spark.read.option("header","true").option("delimiter","|").csv(filename_post).toDF("PersonId","PostId","timestamp")
        //  .select("PersonId","PostId")


        import spark.implicits._
        val IdMappingRDD = spark.sparkContext.textFile(filename_tagMatrix).map(_.split(" ")).map(u=>(u(1),u(3))).toDF("productId","asin")
        val PostHasTag=spark.read.option("header","true").option("delimiter","|").csv(filename_tag).toDF("PostId","productId")
        val Brand=spark.read.option("header","false").option("delimiter",",").csv(filename_brand).toDF("Brand","asin")
        val PersonLikesPost=spark.sparkContext.textFile(filename_post).map(_.split("\\|")).map(u=>(u(0),u(1))).toDF("PersonId","PostId")


        IdMappingRDD.join(PostHasTag,"productId").join(Brand,"asin").select("Brand","PostId")
           .join(PersonLikesPost,"PostId").groupBy("Brand","PersonId").count()
          .repartition(1).write.option("header","false").csv("SocialEngagement")
    */

  }

  case class RFC(frequency: Int, recency: Int)
}
