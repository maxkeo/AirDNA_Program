import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, to_timestamp, year}

object ExportJob {
  def main(args: Array[String]): Unit = {
	// ### **First**: Active properties and their amenities

	//starting local spark session from IngestJob
	val spark = SparkSession.builder.master("local").appName("IngestJob").getOrCreate()

	//reading parquet files from properties folder
	val property_parquet_read = spark.read.parquet("output/properties")

	//filtering only active that is true
	val json_df_active = property_parquet_read.filter("active==true")
	
	//reading amenity parquet file 
	val amenties_parquet_read = spark.read.parquet("output/assignment/amenties/")
	
	//join property parquet and amenity parquet to data frame
	val df_join = json_df_active.join(amenties_parquet_read,Seq("property_id"),"inner")
	val df_join_active_drop = df_join.drop("active","discovered_dt")
	
	//write dataframe as json data with property id and amenties ids list.
		df_join_active_drop.write.json("output/properties_amenties")

	//### **Second**: Summarized count of property discovery dates
	
	//year and month as new column from discovered dates
	val summarized_report = df_join.withColumn("year", year(to_timestamp(col("discovered_dt"), "yyyy-MM-dd"))).withColumn("month", month(to_timestamp(col("discovered_dt"), "yyyy-MM-dd")))
	
	//create new temp view for spark sql
		summarized_report.createOrReplaceTempView("t1")
	
	//spark sql year and month counts of properties for summary report.
	val summarized_report_count = spark.sql("select year,month, count(*) from t1 group by year,month")
	
	//write dataframe to csv files 
		summarized_report_count.write.csv("output/assignment/properties_amenties_summary_report")
	
	spark.stop()
  }
}