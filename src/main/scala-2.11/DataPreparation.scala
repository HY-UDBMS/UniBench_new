import org.apache.spark.sql.{SparkSession, _}

/**
 * Created by chzhang on 24/06/2017.
 */
object DataPreparation {
  def DataProcessing(spark: SparkSession, filePath: String): DataFrame = {
    val OrderDF = spark.read.json(filePath)
    OrderDF
  }
}
