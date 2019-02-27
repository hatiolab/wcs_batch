package com.pluspro.ctrlwcs.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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
	
	/**
	 * Date의 이전 날짜 가져오기 실행.
	 * 
	 * @param date
	 * @return
	 */
	public static String preDate(String date) {
		LocalDate parseDate = LocalDate.parse(date, DateTimeFormatter.BASIC_ISO_DATE);
		return parseDate.minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE);
	}
}
