package com.pluspro.ctrlwcs;

import java.io.FileInputStream;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.pluspro.ctrlwcs.connecter.ConnectionHelper;
import com.pluspro.ctrlwcs.extractor.BoxSorterResult;
import com.pluspro.ctrlwcs.extractor.IExtractor;
import com.pluspro.ctrlwcs.extractor.OnlineResult;
import com.pluspro.ctrlwcs.util.LogUtil;

public class CtrlWcs_Online {

	public static String TAG = CtrlWcs_Online.class.getSimpleName();
	
	public static void main(String[] args) {
		Logger logger = LogUtil.getInstance(TAG);
		
		String confFile = "conf.properties";
		
		Connection ctrlWcsCon = null;		// WCS 통합관제 DB Connection
		Connection wcsCon = null;			// WCS DB Connection
		
		Properties properties = new Properties();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String yyyymmdd = sdf.format(Calendar.getInstance().getTime());
		
		if(args != null && args.length == 1) {
			yyyymmdd = args[0];
		}
		
		try {
			logger.info("Start " + TAG + " ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        	logger.info("Conf File : " + confFile);
        	
        	properties.load(new FileInputStream(confFile));
			
        	StringBuilder sb = new StringBuilder();
        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
        	sb.append("CTRLWCS Database" + System.lineSeparator());
        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
        	sb.append("ctrlwcs.ip : " + properties.getProperty("ctrlwcs.ip") + System.lineSeparator());
        	sb.append("ctrlwcs.port : " + properties.getProperty("ctrlwcs.port") + System.lineSeparator());
        	sb.append("ctrlwcs.sid : " + properties.getProperty("ctrlwcs.sid") + System.lineSeparator());
        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
        	sb.append("WCS Database" + System.lineSeparator());
        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
        	sb.append("wcs.ip : " + properties.getProperty("wcs.ip") + System.lineSeparator());
        	sb.append("wcs.port : " + properties.getProperty("wcs.port") + System.lineSeparator());
        	sb.append("wcs.sid : " + properties.getProperty("wcs.sid") + System.lineSeparator());
        	logger.info(sb.toString());
        	
        	ctrlWcsCon = ConnectionHelper.getOracleConnection(properties.getProperty("ctrlwcs.ip"), properties.getProperty("ctrlwcs.port"), properties.getProperty("ctrlwcs.sid"), properties.getProperty("ctrlwcs.user"), properties.getProperty("ctrlwcs.password"));
			wcsCon = ConnectionHelper.getOracleConnection(properties.getProperty("wcs.ip"), properties.getProperty("wcs.port"), properties.getProperty("wcs.sid"), properties.getProperty("wcs.user"), properties.getProperty("wcs.password"));
        	
			sb = new StringBuilder();
			sb.append("-----------------------------------------------------------" + System.lineSeparator());
			sb.append("DB Connection Instances" + System.lineSeparator());
			sb.append("-----------------------------------------------------------" + System.lineSeparator());
			sb.append("ctrlWcsCon : " + ctrlWcsCon + System.lineSeparator());
			sb.append("wcsCon : " + wcsCon + System.lineSeparator());
			logger.info(sb.toString());
			
			IExtractor extractor = new OnlineResult(yyyymmdd, wcsCon, ctrlWcsCon);
			extractor.extract();
		}catch (Exception e) {
			//e.printStackTrace();
			logger.log(Level.SEVERE, "Error", e);
		} finally {
			if(ctrlWcsCon != null) {
				try { ctrlWcsCon.close(); }catch(Exception e) {}
			}
			
			if(wcsCon != null) {
				try { wcsCon.close(); }catch(Exception e) {}
			}
		}
        
		logger.info("End " + TAG + " ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

	}

}
