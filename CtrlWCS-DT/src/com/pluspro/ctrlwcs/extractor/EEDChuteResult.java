package com.pluspro.ctrlwcs.extractor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.pluspro.ctrlwcs.beans.ChuteResultVo;
import com.pluspro.ctrlwcs.util.LogUtil;

public class EEDChuteResult implements IExtractor{
	
	Logger logger = LogUtil.getInstance();

	String yyyymmdd;
	Connection orgCon;
	Connection trgCon;
	
	public EEDChuteResult(String yyyymmdd, Connection orgCon, Connection trgCon) {
		this.yyyymmdd = yyyymmdd;
		this.orgCon = orgCon;
		this.trgCon = trgCon;
	}
	
	@Override
	public void extract() {
		
		logger.info("Start to extract EED ChuteResult");
		
		ArrayList<ChuteResultVo> list = extractOrg();

		insertTrg(list);
		
		logger.info("Finish to extract EED ChuteResult");
	}

	private ArrayList<ChuteResultVo> extractOrg(){
		ArrayList<ChuteResultVo> list = new ArrayList<>();
		
		Statement stmt = null;
		ResultSet rs = null;
		
		String sql ="SELECT GI_YMD BDATE, 'DT' CENTER_CD,					" + System.lineSeparator() +
					"       'EEDS' EQP_ID, COMPNO CHUTE_NO,					" + System.lineSeparator() +
					"       SORTQTY, SQTY, 									" + System.lineSeparator() +
					"       TO_NUMBER(REPLACE(PER, '%', '')) PROGRESS_RT	" + System.lineSeparator() +
					"  FROM SMS_DT.VW_SUITESUM";
		
		//logger.info(sql);
		
		try {
			stmt = orgCon.createStatement();
			rs = stmt.executeQuery(sql);
			
			while(rs.next()) {
				
				ChuteResultVo vo = new ChuteResultVo();
				
				vo.setBdate(rs.getString("BDATE"));
				vo.setCenterCd(rs.getString("CENTER_CD"));
				vo.setEquipId(rs.getString("EQP_ID"));
				vo.setChuteNo(rs.getString("CHUTE_NO"));
				vo.setProgressRt(rs.getInt("PROGRESS_RT"));
				vo.setPlanQty(rs.getInt("SORTQTY"));
				vo.setQty(rs.getInt("SQTY"));
				
				
				list.add(vo);

				//logger.info(vo.getBdate() + " : " + vo.getCenterCd() + " : " + vo.getEquipId() + " : " + vo.getChuteNo());
				//logger.info(vo.getPlanQty() + " : " + vo.getQty());
			}
			
		} catch(Exception e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, "Error", e);
		} finally {
			if(rs != null) {
				try { rs.close(); }catch(Exception e) {}
			}
			
			if(stmt != null) {
				try { stmt.close(); }catch(Exception e) {}
			}
		}
		
		return list;
	}
			
	private void insertTrg(ArrayList<ChuteResultVo> list) {
		Statement stmt = null;
		
		try {
			
			stmt = trgCon.createStatement();
			
			for(ChuteResultVo vo : list) {
				String sql = "MERGE INTO TB_CHT_RSLT                                                                                              " + System.lineSeparator() +
						 	 "USING(                                                                                                              " + System.lineSeparator() +
						 	 "    SELECT '" + vo.getBdate() + "' BDATE, '" + vo.getCenterCd() + "' CENTER_CD, '" + vo.getEquipId() + "' EQP_ID,   " + System.lineSeparator() +
						 	 "           '" + vo.getChuteNo() + "' CHUTE_NO, " + vo.getProgressRt() + " PROGRESS_RT, 							  " + System.lineSeparator() +
						 	 "           " + vo.getPlanQty() + " PLAN_QTY, " + vo.getQty() + " QTY 												  " + System.lineSeparator() +
						 	 "      FROM DUAL                     																				  " + System.lineSeparator() +
						 	 ") RSLT                                                                                                              " + System.lineSeparator() +
						 	 "ON (TB_CHT_RSLT.BDATE = RSLT.BDATE AND TB_CHT_RSLT.CENTER_CD = RSLT.CENTER_CD                                       " + System.lineSeparator() +
						 	 "    AND TB_CHT_RSLT.EQUIP_ID = RSLT.EQP_ID AND TB_CHT_RSLT.CHUTE_NO = RSLT.CHUTE_NO)                                " + System.lineSeparator() +
						 	 "WHEN MATCHED THEN                                                                                                   " + System.lineSeparator() +
						 	 "     UPDATE SET PROGRESS_RT = RSLT.PROGRESS_RT,                                                                     " + System.lineSeparator() +
						 	 "                PLAN_QTY = RSLT.PLAN_QTY,																			  " + System.lineSeparator() +
						 	 "                QTY = RSLT.QTY,																					  " + System.lineSeparator() +
						 	 "                UPD_DT = SYSDATE                                                                                    " + System.lineSeparator() +
						 	 "WHEN NOT MATCHED THEN                                                                                               " + System.lineSeparator() +
						 	 "     INSERT(BDATE, CENTER_CD, EQUIP_ID, CHUTE_NO, PROGRESS_RT, PLAN_QTY, QTY, REG_DT, UPD_DT)                       " + System.lineSeparator() +
						 	 "     VALUES(RSLT.BDATE, RSLT.CENTER_CD, RSLT.EQP_ID, RSLT.CHUTE_NO, RSLT.PROGRESS_RT, RSLT.PLAN_QTY, RSLT.QTY, SYSDATE, SYSDATE)";
				
				stmt.execute(sql);
			}
			
			logger.info("Extracted " + list.size() + " Datas");
		}catch(Exception e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, "Error", e);
		}finally {
			if(stmt != null) {
				try { stmt.close(); }catch(Exception e) {}
			}
		}
		
		
	}
	
}
