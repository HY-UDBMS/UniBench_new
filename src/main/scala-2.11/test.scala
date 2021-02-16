import SocialNetwork.attributeValueEquals
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, monotonically_increasing_id, rand, udf, when}
import scala.util.Random
import scala.xml.{Node, XML}

object test {
  def main(args: Array[String]): Unit = {
    val scale_factor = "1"
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
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //required by SANSA
      .config("spark.io.compression.codec", "snappy")
      .getOrCreate()

    spark.conf.set("spark.executor.memory","8g")
    spark.conf.set("spark.executor.cores", "3")
    spark.conf.set("spark.cores.max", "3")
    spark.conf.set("spark.driver.memory","8g")

    import spark.implicits._
    { // get rid of DEBUG logs
      import ch.qos.logback.classic.{Level, Logger}
      import org.slf4j.LoggerFactory
      val rootLogger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]
      rootLogger.setLevel(Level.INFO)
    }

    val firstName = spark.sparkContext.textFile("src/main/resources/givennameByCountry").map(_.split("  ")).map(u => (u(0), u(1))).toDF("Country", "firstName")
    val lastName = spark.sparkContext.textFile("src/main/resources/surnameByCountry").map(_.split(",")).map(u => (u(1), u(2))).toDF("Country","lastName")
    val nameByCountry=firstName.join(lastName,"Country").orderBy(rand()).limit(person_size).withColumn("personId",monotonically_increasing_id())
    val PersonwithGenderandBirthDate=nameByCountry.withColumn("gender",when(rand()>0.5,lit("Female")).otherwise("Male")).withColumn("birthday",randBirthDateUdf())
    // Gen 2: person- interest - tag
    // Country
    val Persons=PersonwithGenderandBirthDate.select("personId","Country")
    val CountryIds = spark.sparkContext.textFile("src/main/resources/dicLocations.txt").map(_.split(" ")).map(u => (u(1))).toDF("Country").withColumn("countryId",monotonically_increasing_id())
    val PersonswithCountryId=Persons.join(CountryIds,"Country").toDF()

    // Brand and Item
    val BrandByCountry = spark.sparkContext.textFile("src/main/resources/popularTagByCountry.txt").map(_.split(" ")).map(u => (u(0),u(1))).toDF("countryId","brandId")
    val ItemsByBrand = spark.sparkContext.textFile("src/main/resources/tagMatrix.txt").map(_.split(" ")).map(u => (u(0),u(1))).toDF("brandId","ItemId")

    // Person and Tag, (total count 52433316)
    val Person_hasInterest_Tag=PersonswithCountryId.join(BrandByCountry,"countryId").join(ItemsByBrand,"brandId").select("personId","ItemId").orderBy(rand()).limit(interest_size)
      .orderBy(rand()).limit(interest_size*8/10)
      //.repartition(1).write.option("delimiter", "|").option("header","true").csv("Unibench/Graph_SocialNetwork/PersonHasInterest")
    // Generating random interests
    val randomPersonId=PersonwithGenderandBirthDate.select("personId")
    val randomItemId=ItemsByBrand.select("ItemId")
    val randomInterestgraph=randomPersonId.crossJoin(randomItemId).orderBy(rand()).limit(interest_size*2/10)

    val InterestGraph=Person_hasInterest_Tag.union(randomInterestgraph).count()

    //val InterestGraph=Person_hasInterest_Tag.union(randomInterestgraph).dropDuplicates()
    //InterestGraph.repartition(1).write.option("delimiter", "|").option("header","true").csv("Unibench/Graph_SocialNetwork/PersonHasInterest")
    //randomInterestgraph.orderBy(rand()).limit(interest_size*2/10).repartition(1).write.option("delimiter", "|").option("header","true").csv("Unibench/Graph_SocialNetwork/PersonHasInterest_Random")
    println(InterestGraph)
  }
}
