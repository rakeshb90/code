package com.project.carCostPrediction;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class CarsFeatureSelection {
	public static Dataset<Row> init(){
		System.setProperty("hadoop.home.dir","C://Users/Rakesh/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark=SparkSession.builder().appName("Cars Cost Prediction App")
				          .master("local[*]").config("Key","Value").getOrCreate();
		Dataset<Row> csvData=spark.read()
				         .option("header", true)
				         .option("inferSchema", true)
				         .csv("src/main/resources/input/cars_sampled.csv");
		return csvData;
	}
public static void countNull(Dataset<Row> csvData) {
	for(String col:csvData.columns()) {
		System.out.println("count of null of Column: "+col+" is "+csvData.select(col).where(col(col).isNull()).count());
	}
}
public static Dataset<Row> cleanData(Dataset<Row>csvData){
	csvData=csvData.drop(new String[] {"monthOfRegistration","yearOfRegistration","dateCreated","postalCode","lastSeen","dateCrawled","name"});
	csvData=csvData.na()
			.fill(csvData.groupBy("seller").count().orderBy(desc("seller")).first().get(0).toString(),new String[] {"seller"}).na()
			.fill(csvData.groupBy("offerType").count().orderBy(desc("offerType")).first().get(0).toString(),new String[] {"offerType"}).na()
			.fill(csvData.groupBy("abtest").count().orderBy(desc("abtest")).first().get(0).toString(),new String[] {"abtest"}).na()
			.fill(csvData.groupBy("vehicleType").count().orderBy(desc("vehicleType")).first().get(0).toString(),new String[] {"vehicleType"}).na()
			.fill(csvData.groupBy("gearbox").count().orderBy(desc("gearbox")).first().get(0).toString(),new String[] {"gearbox"}).na()
			.fill(csvData.groupBy("model").count().orderBy(desc("model")).first().get(0).toString(),new String[] {"model"}).na()
			.fill(csvData.groupBy("fuelType").count().orderBy(desc("fuelType")).first().get(0).toString(),new String[] {"fuelType"}).na()
			.fill(csvData.groupBy("brand").count().orderBy(desc("brand")).first().get(0).toString(),new String[] {"brand"}).na()
			.fill(csvData.groupBy("notRepairedDamage").count().orderBy(desc("notRepairedDamage")).first().get(0).toString(),new String[] {"notRepairedDamage"}).na()
			.fill(csvData.stat().approxQuantile("price", new double[] {0.7}, .0000005)[0],new String[] {"price"}).na()
			.fill(csvData.stat().approxQuantile("powerPS", new double[] {0.7}, .0000005)[0],new String[] {"powerPS"}).na()
			.fill(csvData.stat().approxQuantile("kilometer", new double[] {0.7}, .0000005)[0],new String[] {"kilometer"});
	return csvData;
}
public static Dataset<Row> indexing(Dataset<Row>csvData) {
	StringIndexer dataIndex=new StringIndexer()
			.setInputCols(new String[] {"seller","offerType","abtest","vehicleType","gearbox","model","fuelType","brand","notRepairedDamage"})
			.setOutputCols(new String[] {"sellerIndex","offerTypeIndex","abtestIndex","vehicleTypeIndex","gearboxIndex","modelIndex","fuelTypeIndex","brandIndex","notRepairedDamageIndex"});
	csvData=dataIndex.fit(csvData).transform(csvData);
	
	csvData=csvData.drop(new String[] {"seller","offerType","abtest","vehicleType","gearbox","model","fuelType","brand","notRepairedDamage"});

	return csvData;
			
}
public static Dataset<Row> indexToVector(Dataset<Row> csvData){
	OneHotEncoder encoder=new OneHotEncoder()
			.setInputCols(new String[] {"sellerIndex","offerTypeIndex","abtestIndex","vehicleTypeIndex","gearboxIndex","modelIndex","fuelTypeIndex","brandIndex","notRepairedDamageIndex"})
			.setOutputCols(new String[] {"sellerVector","offerTypeVector","abtestVector","vehicleTypeVector","gearboxVector","modelVector","fuelTypeVector","brandVector","notRepairedDamageVector"});
	
	csvData=encoder.fit(csvData).transform(csvData);
	
	return csvData;
}
public static Dataset<Row> getModelInput(Dataset<Row> csvData){
	VectorAssembler assembler=new VectorAssembler()
			.setInputCols(new String[] {"sellerVector","offerTypeVector","abtestVector","vehicleTypeVector","gearboxVector","modelVector","fuelTypeVector","brandVector","notRepairedDamageVector","powerPS","kilometer"})
	        .setOutputCol("features");
   Dataset<Row> modelInput=assembler.transform(csvData)
		            .select("features", "price")
                    .withColumnRenamed("price", "label");
   return modelInput;
}
public static void checkCorrelation(Dataset<Row>csvData) {
	for(String col:csvData.columns()) {
		System.out.println("Correlation of price with "+col+" is: "+Math.abs(csvData.stat().corr("price", col)));
	}
}
	public static void main(String[] args) {
		Dataset<Row> csvData=init();
		csvData.printSchema();
		csvData.show();
		csvData=cleanData(csvData);
		countNull(csvData);
		
		csvData=indexing(csvData);
		csvData.printSchema();
		csvData.show();
		checkCorrelation(csvData);

	}

}
