package com.skc.spark.dev;
import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class StorageManager 
{
    public static void main( String[] args ) throws IOException{
        System.out.println( "Hello World 123!" );

        SparkSession sparkSession = SparkSession.builder()
        							.appName("Spark Files POC")
        							.master("local[4]")
        							.getOrCreate();
        
        Dataset<Row> df = sparkSession.read().csv("/opt/bitnami/spark/examples/jars/employee.csv");
        df.show();
  
        new StorageHTTPServer().startHTTPServer();
    
    }
    
}
