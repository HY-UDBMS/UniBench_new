import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
 * Created by chzhang on 19/06/2017.
 */
object Vendor {
  def GenerateVendor(spark: SparkSession) = {
    // Define the schema of vendor (title,country,industry)
    val id = StructField("id", DataTypes.IntegerType)
    val vendor_name = StructField("name", DataTypes.StringType)
    val country_name = StructField("country", DataTypes.StringType)
    val cv = StructField("cdf", DataTypes.DoubleType)
    val industry = StructField("industry", DataTypes.StringType)
    val schema_vendor = StructType(Array(id, vendor_name, country_name, cv, industry))

    // Table for vendor
    val VendorCatalog = "src/main/resources/PopularSportsBrandByCountry.csv"
    val VendorDF = spark.read.option("header", "false").option("delimiter", ",").option("mode", "DROPMALFORMED").schema(schema_vendor).csv(VendorCatalog)
    VendorDF.select("id", "name", "country", "industry").repartition(1).write.option("header", "true").csv("Unibench/CSV_Vendor")
  }
}
