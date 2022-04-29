package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, split, udf}

object IngestJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("IngestJob").getOrCreate()

    //reading properties.json file
    val json_df = spark.read.json("src/main/resources/assignment/properties.json")

    //write dataframe as parquet file
    json_df.write.parquet("output/properties")

    //reading amenties.txt file
    val df = spark.read.text("src/main/resources/assignment/amenities.txt")

    //split into two columns with blank space separated
    val df2 = df.select(split(col("value"), " ").as("arr"))

    //UDF function to group into two columns
    val udf_slice = udf((x: Seq[String]) => x.grouped(2).toList)

    //use UDF to explode to create two separate columns
    val df3 = df2.select(col("*"), explode(udf_slice(col("arr"))).as("newlist")).select(col("newlist")(0).as("property_id"), col("newlist")(1).as("amentiy_id"))

    //divide Amenities id column into list
    val df4 = df3.select(df3("property_id"), split(col("amentiy_id"), "\\|").as("amenties_id"))

    //use explode funcion to create array into multiple sub rows for same property id.
    val df5 = df4.select(col("property_id"), explode(col("amenties_id")).as("amenty_id"))

    //write dataframe as parquet file for amenties id
    df5.write.parquet("output/assignment/amenties")

    spark.stop()
  }
}
