package com.pluspro.ctrlwcs;

import java.io.FileInputStream;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.pluspro.ctrlwcs.connecter.ConnectionHelper;
import com.pluspro.ctrlwcs.extractor.ChuteResult;
import com.pluspro.ctrlwcs.extractor.ChuteStatus;
import com.pluspro.ctrlwcs.extractor.IExtractor;
import com.pluspro.ctrlwcs.extractor.MPS3Result;
import com.pluspro.ctrlwcs.util.LogUtil;

public class CtrlWcs_MPS3 {
	public static String TAG = CtrlWcs_MPS3.class.getSimpleName();
	
	public static void main(String[] args) {

		Logger logger = LogUtil.getInstance(TAG);
		
		String confFile = "conf.properties";
		
		Connection ctrlWcsCon = null;	// WCS ���հ��� DB Connection
		Connection mps3Con = null;		// MPS3.0 DB Connection
		
		Properties properties = new Properties();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String yyyymmdd = sdf.format(Calendar.getInstance().getTime());
		
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
        	sb.append("mps3.ip : " + properties.getProperty("mps3.ip") + System.lineSeparator());
        	sb.append("mps3.port : " + properties.getProperty("mps3.port") + System.lineSeparator());
        	sb.append("mps3.sid : " + properties.getProperty("mps3.sid") + System.lineSeparator());
        	logger.info(sb.toString());
			
			ctrlWcsCon = ConnectionHelper.getOracleConnection(properties.getProperty("ctrlwcs.ip"), properties.getProperty("ctrlwcs.port"), properties.getProperty("ctrlwcs.sid"), properties.getProperty("ctrlwcs.user"), properties.getProperty("ctrlwcs.password"));
			mps3Con = ConnectionHelper.getOracleConnection(properties.getProperty("mps3.ip"), properties.getProperty("mps3.port"), properties.getProperty("mps3.sid"), properties.getProperty("mps3.user"), properties.getProperty("mps3.password"));
			
			sb = new StringBuilder();
			sb.append("-----------------------------------------------------------" + System.lineSeparator());
			sb.append("DB Connection Instances" + System.lineSeparator());
			sb.append("-----------------------------------------------------------" + System.lineSeparator());
			sb.append("ctrlWcsCon : " + ctrlWcsCon + System.lineSeparator());
			sb.append("mps3Con : " + mps3Con + System.lineSeparator());
			logger.info(sb.toString());
			
			IExtractor extractor = new MPS3Result(yyyymmdd, mps3Con, ctrlWcsCon);
			extractor.extract();
			
		} catch (Exception e) {
			//e.printStackTrace();
			logger.log(Level.SEVERE, "Error", e);
		} finally {
			if(ctrlWcsCon != null) {
				try { ctrlWcsCon.close(); }catch(Exception e) {}
			}
			
			if(mps3Con != null) {
				try { mps3Con.close(); }catch(Exception e) {}
			}
		}
        
		logger.info("End " + TAG + " ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

	}

}
