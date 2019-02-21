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
import com.pluspro.ctrlwcs.extractor.MPS2Result;
import com.pluspro.ctrlwcs.extractor.MPS3Result;
import com.pluspro.ctrlwcs.util.LogUtil;

public class CtrlWcs_MPS2 {
	
	
	
	public static String TAG = CtrlWcs_MPS2.class.getSimpleName();

	public static void main(String[] args) {
		
		if(args == null || args.length != 1 || (!MPS2Result.CMD_KOR.equals(args[0]) && !MPS2Result.CMD_BOX.equals(args[0]) && !MPS2Result.CMD_SUB.equals(args[0]))) {
			Logger _logger = LogUtil.getInstance(TAG);
			String log = System.lineSeparator() + "Argument는 KOR, BOX, SUB 중 하나를 입력하세요."
					   + System.lineSeparator() + "코레일 : KOR"
					   + System.lineSeparator() + "EED 박스 : BOX"
					   + System.lineSeparator() + "EED 소분 : SUB";
			_logger.info(log);
			
			return;
		}
		
		String cmd = args[0];
		String confFile = "conf.properties";
		
		Logger logger = LogUtil.getInstance(TAG + "_" + cmd);
		
		Connection ctrlWcsCon = null;				// WCS 통합관제 DB Connection
		Connection mps2KorailCon = null;			// MPS2.0 코레일 DB Connection
		Connection mps2EEDBoxCon = null;			// MPS2.0 EED 소분 DB Connection
		Connection mps2EEDSubdivisionCon = null;	// MPS2.0 EED 박스 DB Connection
		
		Properties properties = new Properties();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String yyyymmdd = sdf.format(Calendar.getInstance().getTime());
		
		try {
        	
        	logger.info("Start " + TAG + " ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        	logger.info("CMD : " + cmd);
        	logger.info("Conf File : " + confFile);
        	
        	properties.load(new FileInputStream(confFile));
			
        	StringBuilder sb = new StringBuilder();
        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
        	sb.append("CTRLWCS Database" + System.lineSeparator());
        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
        	sb.append("ctrlwcs.ip : " + properties.getProperty("ctrlwcs.ip") + System.lineSeparator());
        	sb.append("ctrlwcs.port : " + properties.getProperty("ctrlwcs.port") + System.lineSeparator());
        	sb.append("ctrlwcs.sid : " + properties.getProperty("ctrlwcs.sid") + System.lineSeparator());
        	
        	if(MPS2Result.CMD_KOR.equals(cmd)) {
	        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
	        	sb.append("MPS2.0 코레일 Database" + System.lineSeparator());
	        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
	        	sb.append("mps2_korail.ip : " + properties.getProperty("mps2_korail.ip") + System.lineSeparator());
	        	sb.append("mps2_korail.port : " + properties.getProperty("mps2_korail.port") + System.lineSeparator());
	        	sb.append("mps2_korail.sid : " + properties.getProperty("mps2_korail.sid") + System.lineSeparator());
        	}else if(MPS2Result.CMD_BOX.equals(cmd)) {
	        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
	        	sb.append("MPS2.0 EED 박스 Database" + System.lineSeparator());
	        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
	        	sb.append("mps2_eedbox.ip : " + properties.getProperty("mps2_eedbox.ip") + System.lineSeparator());
	        	sb.append("mps2_eedbox.port : " + properties.getProperty("mps2_eedbox.port") + System.lineSeparator());
	        	sb.append("mps2_eedbox.sid : " + properties.getProperty("mps2_eedbox.sid") + System.lineSeparator());
        	}else if(MPS2Result.CMD_SUB.equals(cmd)) {
	        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
	        	sb.append("MPS2.0 EED 소분 Database" + System.lineSeparator());
	        	sb.append("-----------------------------------------------------------" + System.lineSeparator());
	        	sb.append("mps2_eedsubdivision.ip : " + properties.getProperty("mps2_eedsubdivision.ip") + System.lineSeparator());
	        	sb.append("mps2_eedsubdivision.port : " + properties.getProperty("mps2_eedsubdivision.port") + System.lineSeparator());
	        	sb.append("mps2_eedsubdivision.sid : " + properties.getProperty("mps2_eedsubdivision.sid") + System.lineSeparator());
        	}
        	logger.info(sb.toString());
			
			ctrlWcsCon = ConnectionHelper.getOracleConnection(properties.getProperty("ctrlwcs.ip"), properties.getProperty("ctrlwcs.port"), properties.getProperty("ctrlwcs.sid"), properties.getProperty("ctrlwcs.user"), properties.getProperty("ctrlwcs.password"));
			if(MPS2Result.CMD_KOR.equals(cmd)) {
				mps2KorailCon = ConnectionHelper.getMsSqlConnection(properties.getProperty("mps2_korail.ip"), properties.getProperty("mps2_korail.port"), properties.getProperty("mps2_korail.sid"), properties.getProperty("mps2_korail.user"), properties.getProperty("mps2_korail.password"));
			}else if(MPS2Result.CMD_BOX.equals(cmd)) {
				mps2EEDBoxCon = ConnectionHelper.getMsSqlConnection(properties.getProperty("mps2_eedbox.ip"), properties.getProperty("mps2_eedbox.port"), properties.getProperty("mps2_eedbox.sid"), properties.getProperty("mps2_eedbox.user"), properties.getProperty("mps2_eedbox.password"));
			}else if(MPS2Result.CMD_SUB.equals(cmd)) {
				mps2EEDSubdivisionCon = ConnectionHelper.getMsSqlConnection(properties.getProperty("mps2_eedsubdivision.ip"), properties.getProperty("mps2_eedsubdivision.port"), properties.getProperty("mps2_eedsubdivision.sid"), properties.getProperty("mps2_eedsubdivision.user"), properties.getProperty("mps2_eedsubdivision.password"));
			}
			
			sb = new StringBuilder();
			sb.append("-----------------------------------------------------------" + System.lineSeparator());
			sb.append("DB Connection Instances" + System.lineSeparator());
			sb.append("-----------------------------------------------------------" + System.lineSeparator());
			sb.append("ctrlWcsCon : " + ctrlWcsCon + System.lineSeparator());
			sb.append("mps2KorailCon : " + mps2KorailCon + System.lineSeparator());
			sb.append("mps2EEDBoxCon : " + mps2EEDBoxCon + System.lineSeparator());
			sb.append("mps2EEDSubdivisionCon : " + mps2EEDSubdivisionCon + System.lineSeparator());
			logger.info(sb.toString());
			
			IExtractor extractor = new MPS2Result(cmd, yyyymmdd, mps2KorailCon, mps2EEDBoxCon, mps2EEDSubdivisionCon, ctrlWcsCon);
			extractor.extract();
			
		} catch (Exception e) {
			//e.printStackTrace();
			logger.log(Level.SEVERE, "Error", e);
		} finally {
			if(ctrlWcsCon != null) {
				try { ctrlWcsCon.close(); }catch(Exception e) {}
			}
			
			if(mps2KorailCon != null) {
				try { mps2KorailCon.close(); }catch(Exception e) {}
			}
			
			if(mps2EEDBoxCon != null) {
				try { mps2EEDBoxCon.close(); }catch(Exception e) {}
			}
			
			if(mps2EEDSubdivisionCon != null) {
				try { mps2EEDSubdivisionCon.close(); }catch(Exception e) {}
			}
		}
        
		logger.info("End " + TAG + " ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

	}

}
