package com.skc.spark.dev;
import java.io.IOException;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.*;

/**
 * Hello world!
 *
 */
public class StorageManager 
{
    public static void main( String[] args ) throws IOException{
        System.out.println( "Hello World 123!" );

  
        new StorageHTTPServer().startHTTPServer();
 /*       
        HttpServer server = HttpServer.create(new InetSocketAddress(8586), 0);
        server.createContext("/test", new MyHandler());
        server.setExecutor(null); // creates a default executor
        server.start();
*/
    
    }
    
}
