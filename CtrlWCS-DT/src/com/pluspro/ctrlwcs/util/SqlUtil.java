package com.pluspro.ctrlwcs.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

public class SqlUtil {
	private static final String DATE_FORMAT = "yyyymmdd";

	public static ArrayList<String> getColumnNameList(ResultSet rs) {

		ArrayList<String> list = new ArrayList<>();

		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			for (int i = 0; i < rsmd.getColumnCount(); i++) {
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
	public static String getPreDate(String date) {
		try {
			Date parseDate = new SimpleDateFormat(DATE_FORMAT).parse(date);

			Calendar calendar = Calendar.getInstance();
			calendar.setTime(parseDate);
			calendar.add(Calendar.DAY_OF_YEAR, -1);

			return new SimpleDateFormat(DATE_FORMAT).format(calendar.getTime());
		} catch (ParseException e) {
			throw new RuntimeException("ParseException", e);
		}
	}
}