package com.pluspro.ctrlwcs.connecter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MsSqlConnecter {
	
	Connection connection;
	
	String ip;
	String port;
	String database;
	String user;
	String password;
	
	public MsSqlConnecter(String ip, String port, String database, String user, String password) {
		this.ip = ip;
		this.port = port;
		this.database = database;
		this.user = user;
		this.password = password;
	}
	
	public Connection open() throws Exception {
		String url = "jdbc:sqlserver://" + ip + ":" + port + ";databaseName=" + database;
        
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");        
        connection = DriverManager.getConnection(url, user, password);
        
        return connection;
	}
	
	public void close() throws Exception {
		connection.close();
	}

	/*
	public static void main(String[] args) {

		if(args == null || args.length != 4) {
			System.out.println("---------------------------------------");
			System.out.println("args is not correct.");
			System.out.println("---------------------------------------");
			
			return;
		}
		
		String server = args[0];
		String database = args[1];
		String user = args[2];
		String password = args[3];
		
		System.out.println("---------------------------------------");
		System.out.println("Server : " + server);
		System.out.println("Database : " + database);
		System.out.println("User : " + user);
		System.out.println("Password : " + password);
		System.out.println("---------------------------------------");
		
		Connection conn = null;
        try {
            //String user = "scott"; 
            //String pw = "tiger";
            //String url = "jdbc:oracle:thin:@localhost:1521:orcl";
        	
        	String url = "jdbc:sqlserver://" + server + ";databaseName=" + database;
            
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");        
            conn = DriverManager.getConnection(url, user, password);
            
            System.out.println("Database�� ����Ǿ����ϴ�.\n");
            
        } catch (ClassNotFoundException cnfe) {
            System.out.println("DB ����̹� �ε� ���� :"+cnfe.toString());
        } catch (SQLException sqle) {
            System.out.println("DB ���ӽ��� : "+sqle.toString());
        } catch (Exception e) {
            System.out.println("Unkonwn error");
            e.printStackTrace();
        }finally{
        	try { conn.close(); } catch (SQLException e) {}
        }
	}
	*/

}
