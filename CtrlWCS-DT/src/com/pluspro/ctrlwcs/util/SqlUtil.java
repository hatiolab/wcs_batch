package com.pluspro.ctrlwcs.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

public class SqlUtil {
	public static ArrayList<String> getColumnNameList(ResultSet rs){
		
		ArrayList<String> list = new ArrayList<>();
		
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			for(int i = 0 ; i < rsmd.getColumnCount() ; i++) {
				String colnumName = rsmd.getColumnName(i + 1);
				list.add(colnumName);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		return list;
	}
}
