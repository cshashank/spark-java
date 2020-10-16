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
	public final String EMPLOYEE_SQL="Select * from employee";
	public final String SKILLS_SQL="Select * from skills";
	public final String ALL_SQL="Select * from all";
	public final String TABLE_EMPLOYEE="employee";
	public final String TABLE_SKILLS="skills";
	public final String TABLE_ALL="all";
	

	public MyHandler(SparkSession sparkSession) {
			System.out.println("In MyHandler");
			this.sparkSession=sparkSession;
	}
	
	
	
	@Override
        public void handle(HttpExchange httpExchange) throws IOException {
			Dataset<Row> emp_df = sparkSession.read().csv("/opt/bitnami/spark/examples/jars/employee.csv");
			Dataset<Row> skill_df = sparkSession.read().csv("/opt/bitnami/spark/examples/jars/skills.csv");
	
            URI requestURI = httpExchange.getRequestURI();
//            String csvTable = requestURI.getQuery();
            String csvTable = fetchTableFromUri(requestURI.getQuery());
            
            
//            String response = "This is the response at "+requestURI + "and request query is "+requestURI.getQuery()
//            +fetchTableData(emp_df);
            
            String response=null;
            Dataset<Row> temp_df = null; 
            StringBuffer resultStr = new StringBuffer();
        	Dataset<Row> employee_df  = sparkSession.read().csv("/opt/bitnami/spark/examples/jars/employee.csv");
        	Dataset<Row> skills_df  = sparkSession.read().csv("/opt/bitnami/spark/examples/jars/skills.csv");
            
            if(csvTable.equalsIgnoreCase("employee")) {
                response = fetchTableData(employee_df);
            }else if(csvTable.equalsIgnoreCase("skills")) {
                response = fetchTableData(skill_df);
            }else if(csvTable.equalsIgnoreCase("all")) {
                response = fetchTableData(employee_df,skill_df);
            }	
            
            if (response == null) {
                response = "Please enter either employee or skills";
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
	public String fetchTableData(Dataset temp_df,Dataset temp1_df) {
		StringBuffer netResult = new StringBuffer();
		netResult.append(fetchTableData(temp_df))
			.append("<br>")
			.append(fetchTableData(temp1_df));
		return netResult.toString();
	}
	
	public String fetchTableFromUri(String uri) {
		String _table="employee";
		if(uri.equalsIgnoreCase(EMPLOYEE_SQL)) return TABLE_EMPLOYEE;
		if(uri.equalsIgnoreCase(SKILLS_SQL)) return TABLE_SKILLS;
		if(uri.equalsIgnoreCase(ALL_SQL)) return TABLE_ALL;
		return _table;
		
	}

}
