import Product.CreateProduct
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import java.io.FileInputStream

import scala.util.Random
import scala.xml.{Node, XML}

object SocialNetwork {
  def attributeValueEquals(value: String)(node: Node) = {
    node.attributes.exists(_.value.text == value)
  }

  def graphGen(spark: SparkSession): Unit = {
    val scale_factor = spark.conf.get("scale_factor").toString
    println("The scale factor is " +scale_factor)
    val xml = XML.loadFile("scale_factors.xml")
    val params = xml \\ "_" filter attributeValueEquals(scale_factor)
    val person_size=(params \ "numPersons" text).toInt
    val interest_size=(params \ "InterestSize" text).toInt
    val knows_size=(params \ "FriendSize" text).toInt
    val BirthStartDate= (params \ "BirthStartDate" text).toInt
    val PostStartDate= (params \ "PostStartDate" text).toInt
    val random1 = new Random(BirthStartDate)
    val random2 = new Random(PostStartDate)
    val randBirthDateUdf = udf(() => (BirthStartDate + random1.nextInt(30)) + "-" + (1 + random1.nextInt(11)).toString.reverse.padTo(2, "0").reverse.mkString + "-" + (1 + random1.nextInt(29)).toString.reverse.padTo(2, "0").reverse.mkString)
    val randPostDateUdf = udf(() => (PostStartDate + random2.nextInt(10)) + "-" + (1 + random2.nextInt(11)).toString.reverse.padTo(2, "0").reverse.mkString + "-" + (1 + random2.nextInt(29)).toString.reverse.padTo(2, "0").reverse.mkString)
    val getRandomElement =udf {()=>
      val gender=Seq("Female","Male")
      gender(random1.nextInt(gender.length))
    }

    import spark.implicits._
    { // get rid of DEBUG logs
      import ch.qos.logback.classic.{Level, Logger}
      import org.slf4j.LoggerFactory
      val rootLogger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]
      rootLogger.setLevel(Level.INFO)
    }

    // Gen 1: person: a join between firstName and LastName scale factor 1: 11000
    //val scale_factor= 1
    val firstName = spark.sparkContext.textFile("src/main/resources/givennameByCountry").map(_.split("  ")).map(u => (u(0), u(1))).toDF("Country", "firstName")
    val lastName = spark.sparkContext.textFile("src/main/resources/surnameByCountry").map(_.split(",")).map(u => (u(1), u(2))).toDF("Country","lastName")
    val nameByCountry=firstName.join(lastName,"Country").orderBy(rand()).limit(person_size).withColumn("personId",monotonically_increasing_id())
    val PersonwithGenderandBirthDate=nameByCountry.withColumn("gender",when(rand()>0.5,lit("Female")).otherwise("Male")).withColumn("birthday",randBirthDateUdf())
    PersonwithGenderandBirthDate.repartition(1).write.option("delimiter", "|").option("header","true").csv("Unibench/CSV_Customer/")

    // Gen 2: person- interest - tag
    // Country
    val Persons=PersonwithGenderandBirthDate.select("personId","Country")
    val CountryIds = spark.sparkContext.textFile("src/main/resources/dicLocations.txt").map(_.split(" ")).map(u => (u(1))).toDF("Country").withColumn("countryId",monotonically_increasing_id())
    val PersonswithCountryId=Persons.join(CountryIds,"Country").toDF()

    // Brand and Item
    val BrandByCountry = spark.sparkContext.textFile("src/main/resources/popularTagByCountry.txt").map(_.split(" ")).map(u => (u(0),u(1))).toDF("countryId","brandId")
    val ItemsByBrand = spark.sparkContext.textFile("src/main/resources/tagMatrix.txt").map(_.split(" ")).map(u => (u(0),u(1))).toDF("brandId","ItemId")

    // Person and Tag, (total count 52433316)
    val Person_hasInterest_Tag=PersonswithCountryId.join(BrandByCountry,"countryId").join(ItemsByBrand,"brandId").select("personId","ItemId")
        .orderBy(rand()).limit(interest_size*8/10)
      //.repartition(1).write.option("delimiter", "|").option("header","true").csv("Unibench/Graph_SocialNetwork/PersonHasInterest")
    // Generating random interests
    val randomPersonId=PersonwithGenderandBirthDate.select("personId")
    val randomItemId=ItemsByBrand.select("ItemId")
    val randomInterestgraph=randomPersonId.crossJoin(randomItemId).orderBy(rand()).limit(interest_size*2/10)
    //randomInterestgraph.repartition(1).write.option("delimiter", "|").option("header","true").csv("Unibench/Graph_SocialNetwork/PersonHasInterest_Random")

    val InterestGraph=Person_hasInterest_Tag.union(randomInterestgraph)
    InterestGraph.repartition(1).write.option("delimiter", "|").option("header","true").csv("Unibench/Graph_SocialNetwork/PersonHasInterest")

    // Gen 3: person- knows -person (total count 40352744)
    val knowsgraph = PersonwithGenderandBirthDate.join(PersonwithGenderandBirthDate,"Country")
      .toDF("1","2","3","personIdsrc","5","6","7","8","personIddst","10","11")
    knowsgraph.select("personIdsrc","personIddst").orderBy(rand()).limit(knows_size*8/10).repartition(1).write.option("delimiter", "|").option("header","true").csv("Unibench/Graph_SocialNetwork/PersonKnowsPerson")

    // Generating friendship randomly
    val randomKnowssrc=PersonwithGenderandBirthDate.select("personId").toDF("personIdsrc")
    val randomKnowsdst=PersonwithGenderandBirthDate.select("personId").toDF("personIddst")
    val randomKnowsgraph=randomKnowssrc.crossJoin(randomKnowsdst)
        .orderBy(rand()).limit(knows_size*2/10).repartition(1).write.option("delimiter", "|").option("header","true").csv("Unibench/Graph_SocialNetwork/PersonKnowsPerson_Random")

    // Divide the taglist to sublists
    def split[Any](xs: List[Any], n: Int): List[List[Any]] = {
      if (xs.isEmpty) Nil
      else (xs take n) :: split(xs drop n, n)
    }

    val divide: (Seq[(String)] => List[List[(String)]]) = (arg: Seq[(String)]) => {
      split(arg.toList, 3)
    }
    val subList = udf(divide)

    // Gen 4: person - has - post
    // person - hasinterest - tag
    val Person_tag = InterestGraph.select("personId","ItemId").toDF("personId","tagId")
                   .groupBy($"personId").agg(collect_list(col("tagId"))).toDF("personId","tagList")
                   .withColumn("subtagList", subList(col("tagList"))).drop(col("tagList"))
                   .withColumn("PostList", explode(col("subtagList"))).drop(col("subtagList"))
                    .withColumn("postId", monotonically_increasing_id())
                   //.withColumn("TagList", explode(col("PostList"))).drop(col("PostList"))

    Person_tag.select("personId","postId").repartition(1).write.option("delimiter", "|").option("header","true").csv("Unibench/Graph_SocialNetwork/PersonHasPost")

    // Gen 5: post - has - tag
    val Post_tag=Person_tag.withColumn("productId", explode(col("PostList"))).drop(col("PostList"))
    Post_tag.select("postId","productId").toDF("postId","tagId").repartition(1).write.option("delimiter", "|").option("header","true").csv("Unibench/Graph_SocialNetwork/PostHasTag")

    // Gen 6: post (total count 184649)
    val ProductDF = CreateProduct(spark)
    Post_tag.join(ProductDF,"productId").groupBy("postId").agg(collect_list("title"))
      .toDF("postId","Description").withColumn("Description",col("Description").cast("string")).withColumn("Date",randPostDateUdf())
      .repartition(1).write.option("delimiter", "|").option("header","true").csv("Unibench/Graph_SocialNetwork/Post")

    // Gen 7: tag
    Utility.Copy("src/main/resources/","Unibench/Graph_SocialNetwork/Tag/tag.csv")
  }
}
/*
    val randomKnowssrc=PersonwithGenderandBirthDate.select("personId").toDF("personIdsrc").sample(true,(knows_size*2)/10).withColumn("row_id", monotonically_increasing_id())
    val randomKnowsdst=PersonwithGenderandBirthDate.select("personId").toDF("personIddst").sample(true,(knows_size*2)/10).withColumn("row_id", monotonically_increasing_id())
    val randomKnowsgraph=randomKnowssrc.join(randomKnowsdst, Seq("row_id")).drop("row_id").dropDuplicates()
    val personKnowsperson=knowsgraph.union(randomKnowsgraph).dropDuplicates().count()
    println("The size is "+personKnowsperson)

        // sampling with Zipfian
    //val PidByBrand = spark.sparkContext.textFile("src/main/resources/tagMatrix.txt").map(_.split(" ")).map(u => (u(0), u(1), u(2).toDouble)).toDF("brand", "id", "prob")
    // cdf sampling
    //val data =PidByBrand.filter($"brand"==='2')
    //data.where(col("prob")>Random.nextDouble()).groupBy().min("prob").show()
    //

        /*
    val randomKnowssrc=PersonwithGenderandBirthDate.select("personId").sample(true,(knows_size)/10).toDF("personIdsrc")
    val randomKnowsdst=PersonwithGenderandBirthDate.select("personId").sample(true,(knows_size)/10).toDF("personIddst")
    val schema = StructType(randomKnowssrc.schema.fields ++ randomKnowsdst.schema.fields)
    val randomKnowsgraph = randomKnowssrc.rdd.zip(randomKnowsdst.rdd).map{
      case (rowLeft, rowRight) => Row.fromSeq(rowLeft.toSeq ++ rowRight.toSeq)}
    val randomKnowsgraphDF = spark.createDataFrame(randomKnowsgraph, schema)
    */

        val schema = StructType(randomKnowssrc.schema.fields ++ randomKnowsdst.schema.fields)
    val zipIndex1 = randomKnowssrc.rdd.zipWithIndex.map((x) =>(x._2, x._1))
    val zipIndex2 = randomKnowsdst.rdd.zipWithIndex.map((x) =>(x._2, x._1))
    val zipped = zipIndex1.join(zipIndex2).map(x => x._2)
    val randomKnowsgraphDF = spark.createDataFrame(zipped.values, schema)
 */