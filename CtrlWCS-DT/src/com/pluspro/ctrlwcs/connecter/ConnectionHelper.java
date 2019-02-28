package com.pluspro.ctrlwcs.connecter;

import java.sql.Connection;

public class ConnectionHelper {
	public static Connection getOracleConnection(String ip, String port, String sid, String user, String password) throws Exception{
		OracleConnecter connecter = new OracleConnecter(ip, port, sid, user, password);
		try {
			return connecter.open();
		}catch(Exception e) {
			throw e;
		}		
	}

	public static Connection getMsSqlConnection(String ip, String port, String sid, String user, String password)  throws Exception{
		MsSqlConnecter connecter = new MsSqlConnecter(ip, port, sid, user, password);
		try {
			return connecter.open();
		}catch(Exception e) {
			throw e;
		}
	}
}
