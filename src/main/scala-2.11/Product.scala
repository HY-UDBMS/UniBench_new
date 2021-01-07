import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
 * Created by chzhang on 19/06/2017.
 */
object Product {
  def WriteToDisk(spark: SparkSession, DF: DataFrame) {
    val ProductDF = CreateProduct(spark)
    ProductDF.select("asin", "title", "price", "imgUrl", "productId", "brand")
      .repartition(1).write.option("header", "true").csv("Unibench/CSV_product")
  }

  // Define the schema of product('asin','title','categories','price','imgUrl','description')
  def CreateProduct(spark: SparkSession): DataFrame = {
    val asin = StructField("asin", DataTypes.StringType)
    val productId = StructField("productId", DataTypes.IntegerType)
    val brand = StructField("brand", DataTypes.IntegerType)
    val title = StructField("title", DataTypes.StringType)
    val price = StructField("price", DataTypes.DoubleType)
    val imgUrl = StructField("imgUrl", DataTypes.StringType)
    val categories = StructField("categories", DataTypes.StringType)
    val description = StructField("description", DataTypes.StringType)
    val feedback = StructField("feedback", DataTypes.StringType)
    val fields = Array(asin, productId, brand, title, price, categories, feedback, imgUrl, description)
    val schema_product = StructType(fields)

    // Load product meta data
    val ProductCatalog = "src/main/resources/metadata_product.csv"
    val ProductDF = spark.read.option("header", "true").option("delimiter", "|")
      .option("mode", "DROPMALFORMED").schema(schema_product).csv(ProductCatalog)
    ProductDF
  }

}
