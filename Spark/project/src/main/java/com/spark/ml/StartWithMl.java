package com.spark.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StartWithMl {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:/Users/Rakesh/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark=SparkSession.builder().appName("FirstSparkMLApp")
				           .master("local[*]")
				           .config("spark.sql.warehouse.dir","C:/Temp/")
				           .getOrCreate();
		
		// data load from file
		Dataset<Row> dataset = spark.read().option("header", true)
				.option("inferSchema",true)
				.csv("src/main/resources/inputdata/kc_house_data.csv");
		
	
//		dataset.show();
//		dataset.printSchema();
		VectorAssembler assembler=new VectorAssembler()
				.setInputCols(new String[] {"bedrooms","bathrooms","sqft_living","floors","sqft_basement"})
				.setOutputCol("features");
		Dataset<Row> dataWithFeature=assembler.transform(dataset);
		Dataset<Row> modelInput=dataWithFeature.select("price","features")
				.withColumnRenamed("price", "label");
		modelInput.show();
		LinearRegression linearRegression=new LinearRegression();
		LinearRegressionModel model=linearRegression.fit(modelInput);
		System.out.println("Model has intercept "+model.intercept()+" and cofficients "+model.coefficients());
		model.transform(modelInput).show();

	}

}
