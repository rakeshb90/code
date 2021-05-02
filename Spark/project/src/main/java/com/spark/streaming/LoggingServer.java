package com.spark.streaming;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

public class LoggingServer {

	public static void main(String[] args) throws IOException, InterruptedException {
//		;
		ServerSocket echoSocket=new ServerSocket(8989);
		while(true) {
			Socket socket=echoSocket.accept();
			for(int trick=0;trick<1000;trick++) {
				PrintWriter out=new PrintWriter(socket.getOutputStream(),true);
				double rand=Math.random();
				String level="DEBUG";
				if(rand<0.0001) {
					level="FETAL";
				}
				else if(rand<0.01) {
					level="ERROR";
					
				}
				else if(rand<0.1) {
					level="WARN";
					
				}
				else if(rand<0.5) {
					level="INFO";
					
				}
				out.print(level+" , "+new Date());
				Thread.sleep(1);
			}
			Thread.sleep(10);
		}
	}

}
