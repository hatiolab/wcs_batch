package com.pluspro.ctrlwcs;

import java.io.FileInputStream;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.pluspro.ctrlwcs.connecter.ConnectionHelper;
import com.pluspro.ctrlwcs.extractor.IExtractor;
import com.pluspro.ctrlwcs.extractor.WMSResult;
import com.pluspro.ctrlwcs.util.LogUtil;
import com.pluspro.ctrlwcs.util.StringJoiner;

public class CtrlWcs_WMS {

	public static String TAG = CtrlWcs_WMS.class.getSimpleName();

	public static void main(String[] args) {
		Logger logger = LogUtil.getInstance(TAG);

		String confFile = "conf.properties";
		String OUT_LINE = " ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++";
		String INNER_LINE = "-----------------------------------------------------------";

		Connection ctrlWcsCon = null; // WCS Monitoring DB Connection
		Connection wmsCon = null; // WMS DB Connection

		Properties properties = new Properties();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String yyyymmdd = sdf.format(Calendar.getInstance().getTime());

		if (args != null && args.length == 1)
			yyyymmdd = args[0];

		try {
			logger.info("Start " + TAG + OUT_LINE);
			logger.info("Conf File : " + confFile);

			properties.load(new FileInputStream(confFile));

			StringJoiner sb = new StringJoiner(System.lineSeparator());
			sb.add(INNER_LINE);
			sb.add("CTRLWCS Database");
			sb.add(INNER_LINE);
			sb.add("ctrlwcs.ip : " + properties.getProperty("ctrlwcs.ip"));
			sb.add("ctrlwcs.port : " + properties.getProperty("ctrlwcs.port"));
			sb.add("ctrlwcs.sid : " + properties.getProperty("ctrlwcs.sid"));
			sb.add(INNER_LINE);
			sb.add("WCS Database");
			sb.add(INNER_LINE);
			sb.add("wms.ip : " + properties.getProperty("wms.ip"));
			sb.add("wms.port : " + properties.getProperty("wms.port"));
			sb.add("wms.sid : " + properties.getProperty("wms.sid"));

			ctrlWcsCon = ConnectionHelper.getOracleConnection(
					properties.getProperty("ctrlwcs.ip"),
					properties.getProperty("ctrlwcs.port"),
					properties.getProperty("ctrlwcs.sid"),
					properties.getProperty("ctrlwcs.user"),
					properties.getProperty("ctrlwcs.password"));

			wmsCon = ConnectionHelper.getMsSqlConnection(
					properties.getProperty("wms.ip"),
					properties.getProperty("wms.port"),
					properties.getProperty("wms.sid"),
					properties.getProperty("wms.user"),
					properties.getProperty("wms.password"));

			sb.add(INNER_LINE);
			sb.add("DB Connection Instances");
			sb.add(INNER_LINE);
			sb.add("ctrlWcsCon : " + ctrlWcsCon);
			sb.add("wmsCon : " + wmsCon);
			logger.info(sb.toString());

			IExtractor extractor = new WMSResult(yyyymmdd, wmsCon, ctrlWcsCon);
			extractor.extract();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Error", e);
		} finally {
			if (ctrlWcsCon != null) {
				try {
					ctrlWcsCon.close();
				} catch (Exception e) {}
			}

			if (wmsCon != null) {
				try {
					wmsCon.close();
				} catch (Exception e) {}
			}
		}

		logger.info("End " + TAG + OUT_LINE);
	}
}
