package com.pluspro.ctrlwcs;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

import com.pluspro.ctrlwcs.connecter.ConnectionHelper;
import com.pluspro.ctrlwcs.connecter.MsSqlConnecter;
import com.pluspro.ctrlwcs.connecter.OracleConnecter;
import com.pluspro.ctrlwcs.extractor.ChuteResult;
import com.pluspro.ctrlwcs.extractor.ChuteStatus;
import com.pluspro.ctrlwcs.extractor.EquipmentStatus;
import com.pluspro.ctrlwcs.extractor.IExtractor;
import com.pluspro.ctrlwcs.util.LogUtil;

public class CtrlWcs_Chute {
	public static String TAG = CtrlWcs_Chute.class.getSimpleName();

	public static void main(String[] args) {
		
		Logger logger = LogUtil.getInstance(TAG);
		
		String confFile = "conf.properties";
		
		Connection ctrlWcsCon = null;	// WCS 통합관제 DB Connection
		Connection wcsCon = null;		// WCS DB Connection
		Connection chuteCon = null;		// Chute DB Connection
		
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
        	//sb.append("ctrlwcs.user : " + properties.getProperty("ctrlwcs.user"));
        	//sb.append("ctrlwcs.password : " + properties.getProperty("ctrlwcs.password"));
        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
        	sb.append("WCS Database" + System.lineSeparator());
        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
        	sb.append("wcs.ip : " + properties.getProperty("wcs.ip") + System.lineSeparator());
        	sb.append("wcs.port : " + properties.getProperty("wcs.port") + System.lineSeparator());
        	sb.append("wcs.sid : " + properties.getProperty("wcs.sid") + System.lineSeparator());
        	//sb.append("wcs.user : " + properties.getProperty("wcs.user"));
        	//sb.append("wcs.password : " + properties.getProperty("wcs.password"));
        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
        	sb.append("CHUTE Database" + System.lineSeparator());
        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
        	sb.append("chute.ip : " + properties.getProperty("chute.ip") + System.lineSeparator());
        	sb.append("chute.port : " + properties.getProperty("chute.port") + System.lineSeparator());
        	sb.append("chute.sid : " + properties.getProperty("chute.sid") + System.lineSeparator());
        	//sb.append("chute.user : " + properties.getProperty("chute.user"));
        	//sb.append("chute.password : " + properties.getProperty("chute.password"));
        	logger.info(sb.toString());
			
			ctrlWcsCon = ConnectionHelper.getOracleConnection(properties.getProperty("ctrlwcs.ip"), properties.getProperty("ctrlwcs.port"), properties.getProperty("ctrlwcs.sid"), properties.getProperty("ctrlwcs.user"), properties.getProperty("ctrlwcs.password"));
			wcsCon = ConnectionHelper.getOracleConnection(properties.getProperty("wcs.ip"), properties.getProperty("wcs.port"), properties.getProperty("wcs.sid"), properties.getProperty("wcs.user"), properties.getProperty("wcs.password"));
			chuteCon = ConnectionHelper.getMsSqlConnection(properties.getProperty("chute.ip"), properties.getProperty("chute.port"), properties.getProperty("chute.sid"), properties.getProperty("chute.user"), properties.getProperty("chute.password"));
			
			sb = new StringBuilder();
			sb.append("-----------------------------------------------------------" + System.lineSeparator());
			sb.append("DB Connection Instances" + System.lineSeparator());
			sb.append("-----------------------------------------------------------" + System.lineSeparator());
			sb.append("ctrlWcsCon : " + ctrlWcsCon + System.lineSeparator());
			sb.append("wcsCon : " + wcsCon + System.lineSeparator());
			sb.append("chuteCon   : " + chuteCon + System.lineSeparator());
			logger.info(sb.toString());
			
			IExtractor chuteStatus = new ChuteStatus(yyyymmdd, chuteCon, ctrlWcsCon);
			chuteStatus.extract();
			
			IExtractor chuteResult = new ChuteResult(yyyymmdd, chuteCon, wcsCon, ctrlWcsCon);
			chuteResult.extract();
			
			IExtractor equipmentStatus = new EquipmentStatus(yyyymmdd, chuteCon, ctrlWcsCon);
			equipmentStatus.extract();
			
		} catch (Exception e) {
			//e.printStackTrace();
			logger.log(Level.SEVERE, "Error", e);
		} finally {
			if(ctrlWcsCon != null) {
				try { ctrlWcsCon.close(); }catch(Exception e) {}
			}
			
			if(wcsCon != null) {
				try { wcsCon.close(); }catch(Exception e) {}
			}
			
			if(chuteCon != null) {
				try { chuteCon.close(); }catch(Exception e) {}
			}
		}
        
		logger.info("End " + TAG + " ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
	}
	
	public static Connection getCtrlWcsConnection(String ip, String port, String sid, String user, String password) throws Exception{
		OracleConnecter connecter = new OracleConnecter(ip, port, sid, user, password);
		try {
			return connecter.open();
		}catch(Exception e) {
			throw e;
		}		
	}

	public static Connection getChuteConnection(String ip, String port, String sid, String user, String password)  throws Exception{
		MsSqlConnecter connecter = new MsSqlConnecter(ip, port, sid, user, password);
		try {
			return connecter.open();
		}catch(Exception e) {
			throw e;
		}
	}
}
