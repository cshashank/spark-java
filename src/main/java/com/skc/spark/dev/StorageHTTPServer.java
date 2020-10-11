package com.skc.spark.dev;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpServer;

public class StorageHTTPServer {
	
	public void startHTTPServer() throws IOException {
	       HttpServer server = HttpServer.create(new InetSocketAddress(8586), 0);
	        server.createContext("/test", new MyHandler());
	        server.setExecutor(null); // creates a default executor
	        server.start();
	 
	}

}
