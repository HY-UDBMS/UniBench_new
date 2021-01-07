import org.apache.spark.sql._

/**
 * Created by chzhang on 19/06/2017.
 */
object Customer {
  def CreateCustomer(spark: SparkSession): DataFrame = {
    val CustomerCatalog = "../ldbc_snb_datagen/test_data/social_network/person_*_0.csv"
    val CustomerDF = spark.read.option("header", "true").option("delimiter", "|").option("mode", "DROPMALFORMED").csv(CustomerCatalog)
    CustomerDF
  }
}
