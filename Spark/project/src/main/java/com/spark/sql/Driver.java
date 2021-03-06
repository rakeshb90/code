package com.spark.sql;

import java.util.Collections;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;


public class Driver {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark=SparkSession.builder()
				.appName("Sql basics")
				.master("local[*]")
				.config("Key","Value")
				.getOrCreate();
		
		Student st=new Student();
		st.setName("Ram");
		st.setAge(20);
		Encoder<Student> studentEncoder=Encoders.bean(Student.class);
//		Dataset<Student> studentData=spark.createDataset(
//				Collections.singletonList(Student), studentEncoder
//				);
		// UDF : (User define function)
		UserDefinedFunction random= udf(
				()->Math.random(),DataTypes.DoubleType
				);
		random.asNonNullable();
		spark.udf().register("random",random);
		spark.sql("SELECT random()").show();
		
	}

}
