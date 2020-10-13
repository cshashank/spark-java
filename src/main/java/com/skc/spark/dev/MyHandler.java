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
			Dataset<Row> emp_df = sparkSession.read().csv("/opt/bitnami/spark/examples/jars/employee.csv");
			Dataset<Row> skill_df = sparkSession.read().csv("/opt/bitnami/spark/examples/jars/skills.csv");
	
            URI requestURI = httpExchange.getRequestURI();
            String csvTable = requestURI.getQuery();
            
            String response = "This is the response at "+requestURI + "and request query is "+requestURI.getQuery()
            +fetchTableData(emp_df);
            
            Dataset<Row> temp_df = null; 
            if(csvTable.equalsIgnoreCase("employee")) {
            	temp_df = sparkSession.read().csv("/opt/bitnami/spark/examples/jars/employee.csv");
            }else if(csvTable.equalsIgnoreCase("skills")) {
            	temp_df = sparkSession.read().csv("/opt/bitnami/spark/examples/jars/skills.csv");
            }	
            
            if (temp_df == null) {
                response = "Please enter either employee or skills";
            }else {
                response = fetchTableData(temp_df);
            }
             
            
            
            httpExchange.sendResponseHeaders(200, response.length());
            OutputStream os = httpExchange.getResponseBody();

            os.write(response.getBytes());
            os.flush();
            os.close();
	}
	
	public String fetchTableData(Dataset temp_df) {
		temp_df.createOrReplaceTempView("tempData");
		Dataset<Row> result = sparkSession.sql("select * from tempData");
		List<Row> rowsList = result.collectAsList();
		Iterator<Row> tempDataDetails = rowsList.iterator();
		StringBuffer responseBuffer =  new StringBuffer();
		while(tempDataDetails.hasNext()) {
			responseBuffer.append(tempDataDetails.next().toString());
		}
//		temp_df.show();
		return responseBuffer.toString();
	}

}
