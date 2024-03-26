package com.spark.challenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, collect_list}
import org.json4s.scalap.scalasig.ClassFileParser.header
import org.apache.spark.sql.functions._


object sparkTest {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("dataFrameBuilder")
      .getOrCreate()

    def main(args: Array[String]): Unit = {

        println("Spark Session Variables:")
        println("App Name: " + spark.sparkContext.appName)
        println("Deploy Mode: " + spark.sparkContext.deployMode)
        println("Master: " + spark.sparkContext.master)

        val df_user_rev: DataFrame = read_csv("src/main/resources/googleplaystore_user_reviews.csv")
        val df_playstore: DataFrame = read_csv("src/main/resources/googleplaystore.csv")

        //Part 1 related functions
        val df1 = part1(df_user_rev)
        df1.show(false)
        
        //Part 2 related functions
        val df2 = part2(df_playstore)
        //save_csv(df2, "src/main/resources/best_apps.csv")
        df2.show(false)
        
        //Part 3 related functions
        val df3 = part3(df_playstore)
        //df3.show(false)
        //save_csv(df3, "src/main/resources/part3_test1.csv")
        
        //Part 4 related functions
        val joined_df = part4(df3, df1)
        joined_df.show(false)
        //save_parquet(joined_df, "src/main/resources/googleplaystore_cleaned.parquet")
        
        //Part 5 related functions
        val df4 = part5(joined_df)
        df4.show(false)
        //save_parquet(df4, "src/main/resources/googleplaystore_metrics.parquet")
    }

    ///Read Functions

    //try to do a function that reads the csv file and returns a dataframe
    /**
     * Read a CSV file and return a DataFrame
     * @param file_path
     * @return
     */
    def read_csv(file_path: String): DataFrame = {
        val df = spark.read
          .format("csv")
          .option("header", "true") // Use first line of the file as a header
          .option("inferSchema","true")
          .csv(file_path)

        return df
    }

    /// Save Functions

    /**
     * Save a DataFrame to a CSV file
     * @param df
     * @param file_path
     */
    private def save_csv(df: DataFrame, file_path: String): Unit = {
        df.write.format("csv")
          .option("header", "true")
          //.option("delimiter", "§")
          .save(file_path)
    }

    /**
     * Save a DataFrame to a Parquet file
     * @param df
     * @param file_path
     */
    def save_parquet(df: DataFrame, file_path: String): Unit = {
        df.write
          .option("compression", "gzip")
          .parquet(file_path)
    }

    /**
     * Part 1: From googleplaystore_user_reviews.csv create a Dataframe (df_1)
     * @param input_df
     * @return
     */
    def part1(input_df : DataFrame): DataFrame = {

        // Create a DataFrame and read and input the data from the CSV file
        // obs - i had to take out a coma (,) from the 1st line of the csv file to be able to read it properly
        val df = input_df
          .select(col("App").cast("String"), col("Sentiment_Polarity").cast("Double"))
          .na.fill(0) // Trade de NaN values for 0
          .groupBy("App").avg("Sentiment_Polarity")
          .withColumnRenamed("avg(Sentiment_Polarity)", "Average_Sentiment_Polarity")
          //.toDF()

        //df1.toDF("App", "Average_Sentiment_Polarity")

        // Show the DataFrame
        //input_df.show(false)
        //df1.show(100, truncate = false) // Show the first 100 rows and don't truncate the result

        return df
    }

    /**
     * Part 2: From googleplaystore.csv create a Dataframe (df_2) with ratigns > 4.0 and fill NaN values with 0
     * @param input_df
     * @return
     */
    def part2(input_df : DataFrame): DataFrame = {
        val df = input_df
          .na.fill(0)
          .where(col("Rating") >= 4.0)
          .orderBy(col("Rating").desc)

        //df.show(false)

        return df
    }

    /**
     * Part 3: From googleplaystore.csv create a Dataframe (df_3) with the app with the most reviews
     * @param input_df
     * @return
     */
    def part3(input_df : DataFrame): DataFrame = {
        val maxReviewsDF = input_df
          .select(col("App").cast("String"), col("Category").cast("String"), col("Rating").cast("Double"), col("Reviews").cast("Long"), col("Size").cast("Double"), col("Installs").cast("String"), col("Type").cast("String"), col("Price").cast("Double"), col("Content Rating").cast("String"), col("Genres").cast("String"), col("Last Updated").cast("Date"), col("Current Ver").cast("String"), col("Android Ver").cast("String"))
          .groupBy("App")
          .agg(concat_ws(";", collect_set("Category")) as "MaxCategories", concat_ws(";", collect_set("Genres")) as "MaxGenres", max("Reviews") as "MaxReviews")
          .toDF()


        val df = input_df
          .join(maxReviewsDF, input_df("App") === maxReviewsDF("App") && input_df("Reviews") === maxReviewsDF("MaxReviews"))
          .drop(maxReviewsDF("App"))  // Drop the duplicate "App" column
          //.drop("Reviews")  // We don't need the "MaxReviews" column anymore
          //.drop("Categories")  // We don't need the "Categories" column anymore
          //.drop("Genres")  // We don't need the "Genres" column anymore
          .select("App", "MaxCategories", "Rating", "MaxReviews", "Size", "Installs", "Type", "Price", "Content Rating", "MaxGenres", "Last Updated", "Current Ver", "Android Ver")
          .withColumnRenamed("MaxCategories", "Categories")
          .withColumnRenamed("MaxReviews", "Reviews")
          .withColumnRenamed("MaxGenres", "Genres")


        return df
    }

    /**
     * Part 4: Join df_1 and df_3 on the App column
     * @param input_df1
     * @param input_df2
     * @return
     */
    def part4(input_df1 : DataFrame, input_df2 : DataFrame): DataFrame = {
        val final_df = input_df1
          .join(input_df2, input_df1("App") === input_df2("App"))
          .drop(input_df2("App"))  // Drop the duplicate "App" column

        final_df
    }

    /**
     * Part 5: From the joined DataFrame, create a DataFrame with the count of apps per genre, the average rating and the average sentiment polarity
     * @param input_df
     * @return
     */
    def part5(input_df : DataFrame): DataFrame = {
        val count_df : DataFrame = input_df
          .select("Genres", "Rating", "Average_Sentiment_Polarity")
          .groupBy("Genres")
          //.count()/*.as("Count")*/
          .agg(count("Genres"), avg("Rating"), avg("Average_Sentiment_Polarity"))
          //.select("Genres", "Count", "Rating", "Average_Sentiment_Polarity")
          .withColumnRenamed("count(Genres)", "Count")
          .withColumnRenamed("avg(Rating)", "Average_Rating")
          .withColumnRenamed("avg(Average_Sentiment_Polarity)", "Average_Sentiment_Polarity")

        return count_df
    }

}
