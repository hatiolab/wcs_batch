package com.pluspro.ctrlwcs.connecter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleConnecter {
	
	Connection connection;
	
	String ip;
	String port;
	String sid;
	String user;
	String password;
	
	public OracleConnecter(String ip, String port, String sid, String user, String password) {
		this.ip = ip;
		this.port = port;
		this.sid = sid;
		this.user = user;
		this.password = password;		
	}
	
	public Connection open() throws Exception {
		String url = "jdbc:oracle:thin:@" + ip + ":" + port + ":" + sid;
        
        Class.forName("oracle.jdbc.driver.OracleDriver");        
        connection = DriverManager.getConnection(url, user, password);
        
        return connection;
	}
	
	public void close() throws Exception {
		connection.close();
	}
	
	/*
	public static void main(String[] args) {
		
		if(args == null || args.length != 3) {
			System.out.println("---------------------------------------");
			System.out.println("args is not correct.");
			System.out.println("---------------------------------------");
			
			return;
		}
		
		String server = args[0];
		String user = args[1];
		String password = args[2];
		
		System.out.println("---------------------------------------");
		System.out.println("Server : " + server);
		System.out.println("User : " + user);
		System.out.println("Password : " + password);
		System.out.println("---------------------------------------");
		
		Connection conn = null;
        try {
            //String user = "scott"; 
            //String pw = "tiger";
            //String url = "jdbc:oracle:thin:@localhost:1521:orcl";
        	
        	String url = "jdbc:oracle:thin:@" + server;
            
            Class.forName("oracle.jdbc.driver.OracleDriver");        
            conn = DriverManager.getConnection(url, user, password);
            
            System.out.println("Database에 연결되었습니다.\n");
            
        } catch (ClassNotFoundException cnfe) {
            System.out.println("DB 드라이버 로딩 실패 :"+cnfe.toString());
        } catch (SQLException sqle) {
            System.out.println("DB 접속실패 : "+sqle.toString());
        } catch (Exception e) {
            System.out.println("Unkonwn error");
            e.printStackTrace();
        }finally{
        	try { conn.close(); } catch (SQLException e) {}
        }

	}
	*/
}
