package com.project.housepricepridiction;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// Features Selection or Field selection for model
public class HousePriceField {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:/Users/Rakesh/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark=SparkSession.builder().appName("House price prediction")
				.master("local[*]").config("spark.some.config.option", "some-value")
				.getOrCreate();
		Dataset<Row> input=spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/input/kc_house_data.csv");
//		input.describe().show();
		// delete all columns which is not related to goal column(Price)
		input=input.drop("id","date","waterfront","view","condition","grade","yr_renovated","zipcode","lat","long");
//		for(String col:input.columns()) {
//		System.out.println("Correlation value between price and "+col+" is "+input.stat().corr("price", col));
//		}
       input=input.drop("sqft_lot","sqft_living15","sqft_lot15","yr_built");
       for(String col1:input.columns()) {
    	   for(String col2:input.columns()) {
    		   System.out.println("Correlation value between"+ col1 +" and "+col2+" is "+input.stat().corr(col1, col2));
    	   }
       }
	}

}
