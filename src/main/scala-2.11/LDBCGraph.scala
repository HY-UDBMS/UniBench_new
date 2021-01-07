import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by chzhang on 19/06/2017.
 */
object LDBCGraph {
  def read(spark: SparkSession, file: String): DataFrame = {
    val KnowsGraph = "../ldbc_snb_datagen/parameter_curation/social_network/person_knows_person_*_0.csv"
    val Post = "../ldbc_snb_datagen/parameter_curation/social_network/post_0_0"
    //val Post_hasCreateor_person="../ldbc_snb_datagen/parameter_curation/social_network/person_knows_person_*_0.csv"
    val Person_likes_Post = "../ldbc_snb_datagen/parameter_curation/social_network/person_likes_post_*_0.csv"
    val post_hasTag_tag = "../ldbc_snb_datagen/parameter_curation/social_network/post_hasTag_tag_*_0.csv"

    val GraphDF = spark.read.option("header", "true").option("delimiter", "|").csv(file)
    GraphDF
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("LDBCGraph transformer").setMaster("local")
    val spark = SparkSession.builder()
      .master("local[4]")
      .getOrCreate()

    {
      val knows = read(spark, "../ldbc_snb_datagen/parameter_curation/social_network/person_*_0.csv")
      knows.repartition(1).write.option("header", "true").option("delimiter", "|").csv("Unibench/person_*_0.csv")
    }

    {
      val knows = read(spark, "../ldbc_snb_datagen/parameter_curation/social_network/person_knows_person_*_0.csv")
        .withColumnRenamed("Person.id0", "from")
        .withColumnRenamed("Person.id1", "to")
      knows.repartition(1).write.option("header", "true").option("delimiter", "|").csv("Unibench/person_knows_person_*_0.csv")
    }

    {
      val post = read(spark, "../ldbc_snb_datagen/parameter_curation/social_network/post_*_0.csv")
      post.repartition(1).write.option("header", "true").option("delimiter", "|").csv("Unibench/post_*_0.csv")
    }

    {
      val hasCreator = read(spark, "../ldbc_snb_datagen/parameter_curation/social_network/post_hasCreator_person_*_0.csv")
        .withColumnRenamed("Post.id", "PostId")
        .withColumnRenamed("Person.id", "PersonId")
      hasCreator.repartition(1).write.option("header", "true").option("delimiter", "|").csv("Unibench/post_hasCreator_person_*_0.csv")
    }

    {
      val hasTag = read(spark, "../ldbc_snb_datagen/parameter_curation/social_network/post_hasTag_tag_*_0.csv")
        .withColumnRenamed("Post.id", "PostId")
        .withColumnRenamed("Tag.id", "TagId")
      hasTag.repartition(1).write.option("header", "true").option("delimiter", "|").csv("Unibench/post_hasTag_tag_*_0.csv")
    }

    spark.stop()
  }
}
