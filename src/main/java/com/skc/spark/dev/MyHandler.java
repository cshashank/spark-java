package com.skc.spark.dev;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import com.sun.net.httpserver.HttpExchange;

public class MyHandler implements com.sun.net.httpserver.HttpHandler {
	
	SparkSession sparkSession;

	public MyHandler(SparkSession sparkSession) {
			System.out.println("In MyHandler");
			this.sparkSession=sparkSession;
	}
	
	
	
	@Override
        public void handle(HttpExchange httpExchange) throws IOException {
			System.out.println("In MyHandler.handle()");
			Dataset<Row> emp_df = sparkSession.read().csv("/opt/bitnami/spark/examples/jars/employee.csv");
			emp_df.createOrReplaceTempView("empData");
			Dataset<Row> result = sparkSession.sql("select * from empData");
			List<Row> rowsList = result.collectAsList();
			Iterator<Row> empDetails = rowsList.iterator();
			StringBuffer responseBuffer =  new StringBuffer();
			while(empDetails.hasNext()) {
				responseBuffer.append(empDetails.next().toString());
			}
	
	        emp_df.show();	
            
            URI requestURI = httpExchange.getRequestURI();
            String response = "This is the response at "+requestURI + "and request query is "+requestURI.getQuery()+responseBuffer;
            
            httpExchange.sendResponseHeaders(200, response.length());
            OutputStream os = httpExchange.getResponseBody();

            os.write(response.getBytes());
            os.flush();
            os.close();
	}
	
	public String fetchTableData(Dataset df) {
		emp_df.createOrReplaceTempView("empData");
		Dataset<Row> result = sparkSession.sql("select * from empData");
		List<Row> rowsList = result.collectAsList();
		Iterator<Row> empDetails = rowsList.iterator();
		StringBuffer responseBuffer =  new StringBuffer();
		while(empDetails.hasNext()) {
			responseBuffer.append(empDetails.next().toString());
		}
		
		return responseBuffer.toString();
	}

}
