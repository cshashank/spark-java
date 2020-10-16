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
	    Dataset<Row> df = sparkSession.read().csv("/opt/bitnami/spark/examples/jars/employee.csv");
	    df.show();	
	}
	
	public void startHTTPServer(SparkSession sparkSession) throws IOException {
			int port = 8586;
			HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
	        server.createContext("/radar", new MyHandler(sparkSession));
	        server.setExecutor(null); // creates a default executor
	        server.start();
	        System.out.println("Started server at "+ port + " on context /text");
	        System.out.println("/radar?Select * from employee or /radar?Select * from skills");
	}
	

}
