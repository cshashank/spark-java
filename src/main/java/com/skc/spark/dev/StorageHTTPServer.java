package com.skc.spark.dev;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpServer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StorageHTTPServer {
	
	public StorageHTTPServer() {}
	public StorageHTTPServer(SparkSession sparkSession) {
		System.out.println("In StorageHTTPServer");
	    Dataset<Row> df = sparkSession.read().csv("/opt/bitnami/spark/examples/jars/employee.csv");
	    df.show();	
	}
	
	public void startHTTPServer(SparkSession sparkSession) throws IOException {
	       HttpServer server = HttpServer.create(new InetSocketAddress(8586), 0);
	        server.createContext("/test", new MyHandler(sparkSession));
	        server.setExecutor(null); // creates a default executor
	        server.start();
	 
	}
	

}
