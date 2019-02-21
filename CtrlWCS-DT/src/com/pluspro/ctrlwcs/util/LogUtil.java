package com.pluspro.ctrlwcs.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.pluspro.ctrlwcs.CtrlWcs_Chute;

public class LogUtil {
	
	private static Logger logger = null;
	
	public static Logger getInstance() {
		return getInstance(null);
	}
	
	public static Logger getInstance(String name) {
		if(logger == null) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
				String yyyymmdd = sdf.format(Calendar.getInstance().getTime());
				
				FileHandler fh = new FileHandler("logs/" + name + "_" + yyyymmdd + ".log", true);
	        	fh.setFormatter(new SimpleFormatter());
	        	
	    		logger = Logger.getLogger(name);
	    		logger.setUseParentHandlers(true);
	    		logger.addHandler(fh);
			}catch(Exception e) {
				
			}
		}
		
		return logger;
	}
	
	public static void clear() {
		logger = null;
	}
}
