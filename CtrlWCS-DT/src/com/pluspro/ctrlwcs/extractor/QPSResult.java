package com.pluspro.ctrlwcs.extractor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.pluspro.ctrlwcs.beans.EquipmentResultVo;
import com.pluspro.ctrlwcs.util.LogUtil;

public class QPSResult implements IExtractor {

	Logger logger = LogUtil.getInstance();
	
	String yyyymmdd;
	Connection orgCon;
	Connection trgCon;
	
	public QPSResult(String yyyymmdd, Connection orgCon, Connection trgCon) {
		this.yyyymmdd = yyyymmdd;
		this.orgCon = orgCon;
		this.trgCon = trgCon;
	}
	
	@Override
	public void extract() {
		logger.info("Start to extract QPS");
		
		ArrayList<EquipmentResultVo> list = extractOrg();
		insertTrg(list);
		
		logger.info("End to extract QPS");
	}

	private ArrayList<EquipmentResultVo> extractOrg(){
		ArrayList<EquipmentResultVo> list = new ArrayList<>();
		
		Statement stmt = null;
		ResultSet rs = null;
		
		String sql ="SELECT B.*,                                                                                                            		" + System.lineSeparator() +
					"       (SELECT MAX(WRK_STRT_DT) FROM TB_WCS_WRK_BTCH WHERE EQP_ID = B.EQUIP_ID AND BTCH_SEQ = B.BTCH_SEQ) START_TM,    		" + System.lineSeparator() +
					"       (SELECT MAX(WRK_CMPT_DT) FROM TB_WCS_WRK_BTCH WHERE EQP_ID = B.EQUIP_ID AND BTCH_SEQ = B.BTCH_SEQ) END_TM				" + System.lineSeparator() +
					"  FROM (																														" + System.lineSeparator() +
				    "        SELECT A.BDATE,																										" + System.lineSeparator() +
					"               A.CENTER_CD,																									" + System.lineSeparator() +
					"               A.EQUIP_ID,																										" + System.lineSeparator() +
					"               (SELECT COM_DETAIL_NM FROM TB_COMM_CODE_MST																		" + System.lineSeparator() + 
					"                 WHERE COM_HEAD_CD = 'CENTER_CD' AND COM_DETAIL_CD = A.CENTER_CD)CENTER_NM,									" + System.lineSeparator() + 
					"               (SELECT EQP_NM FROM TB_COMM_EQUIP_MST																			" + System.lineSeparator() +
					"                 WHERE EQP_ID = A.EQUIP_ID) EQUIP_NM,																			" + System.lineSeparator() +
					"               A.ORD,																											" + System.lineSeparator() +
					"               MAX(A.BTCH_SEQ) BTCH_SEQ,                                                                                       " + System.lineSeparator() +
					"               COUNT(DISTINCT A.HEAP_PLAN_INV) HEAP_PLAN_INV,																	" + System.lineSeparator() + 
					"               COUNT(DISTINCT A.HEAP_INV) HEAP_INV,																			" + System.lineSeparator() +
					"               COUNT(DISTINCT A.PLAN_INV) PLAN_INV,																			" + System.lineSeparator() +
					"               COUNT(DISTINCT A.INV) INV,																						" + System.lineSeparator() +
					"               COUNT(DISTINCT A.WORKABLE_INV) WORKABLE_INV,																	" + System.lineSeparator() +
					"               COUNT(DISTINCT A.MULTI_REST_INV) MULTI_REST_INV																	" + System.lineSeparator() +
					"          FROM (SELECT TB_WCS_ORD_HDR.WRK_IDCT_YMD BDATE,																		" + System.lineSeparator() +
					"                       TB_WCS_ORD_HDR.CENTER_CD CENTER_CD,																		" + System.lineSeparator() +
					"	                    TB_QPS_SORT_PLAN_HDR.EQP_ID EQUIP_ID,																	" + System.lineSeparator() +
					"                       TB_QPS_SORT_PLAN_HDR.WAV_NO ORD,																		" + System.lineSeparator() +
					"                       TB_QPS_SORT_PLAN_DTL.BTCH_SEQ BTCH_SEQ,                                                            		" + System.lineSeparator() +
					"	                    TB_QPS_SORT_PLAN_DTL.SKU_CD HEAP_PLAN_INV,																" + System.lineSeparator() +
					"	                    DECODE(MIN(TB_QPS_SORT_PLAN_HDR.REP_COMP_YN), 'Y', TB_QPS_SORT_PLAN_DTL.SKU_CD, NULL) HEAP_INV,			" + System.lineSeparator() + 
					"                       TB_WCS_ORD_HDR.ORD_NO PLAN_INV,																			" + System.lineSeparator() +
					"	                    DECODE(SUM(TB_WCS_ORD_HDR.PLAN_QTY), SUM(TB_WCS_ORD_HDR.RSLT_QTY), TB_WCS_ORD_HDR.ORD_NO, NULL) INV,	" + System.lineSeparator() + 
					"	                    DECODE(MIN(TB_QPS_SORT_PLAN_HDR.REP_COMP_YN), 															" + System.lineSeparator() +
					"                              'Y', 																							" + System.lineSeparator() +
					"                              DECODE(MIN(TB_QPS_SORT_PLAN_HDR.WRK_POSB_YN), 													" + System.lineSeparator() + 
					"                                     'Y', 																						" + System.lineSeparator() +
					"                                     TB_WCS_ORD_HDR.ORD_NO, 																	" + System.lineSeparator() +
					"                                     NULL)) WORKABLE_INV,																		" + System.lineSeparator() + 
					"                       DECODE(MIN(TB_QPS_SORT_PLAN_HDR.MULT_TMPT_PACKG_YN),                                                    " + System.lineSeparator() +
					"	                           'Y', 																							" + System.lineSeparator() +
					"		                       DECODE(SUM(TB_WCS_ORD_HDR.PLAN_QTY), SUM(TB_WCS_ORD_HDR.RSLT_QTY), NULL, TB_WCS_ORD_HDR.ORD_NO),	" + System.lineSeparator() + 
					"		                       NULL) MULTI_REST_INV																				" + System.lineSeparator() +
					"                  FROM TB_WCS_ORD_HDR, QPSADM.TB_QPS_SORT_PLAN_HDR, QPSADM.TB_QPS_SORT_PLAN_DTL								" + System.lineSeparator() + 
					"	              WHERE TB_WCS_ORD_HDR.WRK_IDCT_YMD = '" + yyyymmdd + "'														" + System.lineSeparator() +
					"	                AND TB_QPS_SORT_PLAN_HDR.CENTER_CD = 'DT'																	" + System.lineSeparator() +
					"	                AND TB_QPS_SORT_PLAN_HDR.EQP_ID IN ('DQ01', 'DQ02', 'DQ03', 'DQ04')											" + System.lineSeparator() + 
					"                   AND TB_QPS_SORT_PLAN_HDR.CENTER_CD = TB_WCS_ORD_HDR.CENTER_CD												" + System.lineSeparator() +
					"	                AND TB_QPS_SORT_PLAN_HDR.ORD_NO = TB_WCS_ORD_HDR.ORD_NO														" + System.lineSeparator() +
					"	                AND TB_QPS_SORT_PLAN_HDR.CENTER_CD = TB_QPS_SORT_PLAN_DTL.CENTER_CD											" + System.lineSeparator() +
					"	                AND TB_QPS_SORT_PLAN_HDR.EQP_ID = TB_QPS_SORT_PLAN_DTL.EQP_ID												" + System.lineSeparator() +
					"	                AND TB_QPS_SORT_PLAN_HDR.BTCH_SEQ = TB_QPS_SORT_PLAN_DTL.BTCH_SEQ											" + System.lineSeparator() +
					"	                AND TB_QPS_SORT_PLAN_HDR.WRK_NO = TB_QPS_SORT_PLAN_DTL.WRK_NO												" + System.lineSeparator() +
					"	              GROUP BY TB_WCS_ORD_HDR.WRK_IDCT_YMD,																			" + System.lineSeparator() +
					"                          TB_WCS_ORD_HDR.CENTER_CD,																			" + System.lineSeparator() +
					"	                       TB_QPS_SORT_PLAN_HDR.EQP_ID,																			" + System.lineSeparator() +
					"                          TB_QPS_SORT_PLAN_HDR.WAV_NO,																			" + System.lineSeparator() +
					"	                       TB_QPS_SORT_PLAN_DTL.SKU_CD,																			" + System.lineSeparator() +
					"	                       TB_WCS_ORD_HDR.ORD_NO, TB_QPS_SORT_PLAN_DTL.BTCH_SEQ) A																				" + System.lineSeparator() +
					"         GROUP BY BDATE, CENTER_CD, EQUIP_ID, ORD) B";
		
		try {
			stmt = orgCon.createStatement();
			rs = stmt.executeQuery(sql);
			
			while(rs.next()) {
				EquipmentResultVo vo = new EquipmentResultVo();
				vo.setBdate(rs.getString("BDATE"));
				vo.setCenterCd(rs.getString("CENTER_CD"));
				vo.setEquipId(rs.getString("EQUIP_ID"));
				vo.setCenterNm(rs.getString("CENTER_NM"));
				vo.setEquipNm(rs.getString("EQUIP_NM"));
				vo.setOrd(rs.getString("ORD"));
				vo.setHeapPlanInv(rs.getInt("HEAP_PLAN_INV"));
				vo.setHeapInv(rs.getInt("HEAP_INV"));
				vo.setPlanInv(rs.getInt("PLAN_INV"));
				vo.setInv(rs.getInt("INV"));
				vo.setWorkableInv(rs.getInt("WORKABLE_INV"));
				vo.setMultiRestInv(rs.getInt("MULTI_REST_INV"));
				vo.setStartTm(rs.getTimestamp("START_TM"));
				vo.setEndTm(rs.getTimestamp("END_TM"));
				
				//logger.info(vo.getBdate() + " : " + vo.getCenterCd() + " : " + vo.getEquipId() + " : " + vo.getChuteNo() + " : " + vo.getStatus());
				
				list.add(vo);
			}
			
		} catch(Exception e) {
			//e.printStackTrace();
			logger.log(Level.SEVERE, e.getMessage(), e);
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
	
	
	private void insertTrg(ArrayList<EquipmentResultVo> list) {
		Statement stmt = null;
		
		try {
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
			
			stmt = trgCon.createStatement();
			
			for(EquipmentResultVo vo : list) {
				String startTmSql = null;
				String endTmSql = null;
				
				if(vo.getStartTm() != null) startTmSql = "TO_DATE('" + sdf.format(vo.getStartTm()) + "', 'yyyymmdd hh24miss')";
				else startTmSql = "NULL";
				
				if(vo.getEndTm() != null) endTmSql = "TO_DATE('" + sdf.format(vo.getEndTm()) + "', 'yyyymmdd hh24miss')";
				else endTmSql = "NULL";
				
				String sql ="MERGE INTO TB_EQP_RSLT" + System.lineSeparator() +
							"USING(																													" + System.lineSeparator() +
							"    SELECT '" + vo.getBdate() + "' BDATE, '" + vo.getCenterCd() + "'CENTER_CD, '" + vo.getEquipId() + "' EQUIP_ID,		" + System.lineSeparator() +
							"           '" + vo.getCenterNm() + "' CENTER_NM, '" + vo.getEquipNm() + "' EQUIP_NM, '" + vo.getOrd() + "' ORD,		" + System.lineSeparator() +
							"           " + vo.getHeapPlanInv() + " HEAP_PLAN_INV, " + vo.getHeapInv() + " HEAP_INV,								" + System.lineSeparator() +
							"           " + vo.getPlanInv() + " PLAN_INV, " + vo.getInv() + " INV,													" + System.lineSeparator() +
							"           " + vo.getWorkableInv() + " WORKABLE_INV, " + vo.getMultiRestInv() + " MULTI_REST_INV,						" + System.lineSeparator() +
							"           " + startTmSql + " START_TM, " + endTmSql + " END_TM														" + System.lineSeparator() +
							"      FROM DUAL																										" + System.lineSeparator() +
							") ORG 																													" + System.lineSeparator() +
							"ON (TB_EQP_RSLT.BDATE = ORG.BDATE AND TB_EQP_RSLT.CENTER_CD = ORG.CENTER_CD AND 										" + System.lineSeparator() +
							"    TB_EQP_RSLT.EQUIP_ID = ORG.EQUIP_ID AND TB_EQP_RSLT.ORD = ORG.ORD) 												" + System.lineSeparator() +
							"WHEN MATCHED THEN 																										" + System.lineSeparator() +
							"  UPDATE SET HEAP_PLAN_INV = ORG.HEAP_PLAN_INV,																		" + System.lineSeparator() +
							"             HEAP_INV = ORG.HEAP_INV,																					" + System.lineSeparator() +
							"             PLAN_INV = ORG.PLAN_INV,																					" + System.lineSeparator() +
							"             INV = ORG.INV,																							" + System.lineSeparator() +
							"             WORKABLE_INV = ORG.WORKABLE_INV,																			" + System.lineSeparator() +
							"             MULTI_REST_INV = ORG.MULTI_REST_INV,																		" + System.lineSeparator() +
							"             START_TM = ORG.START_TM,																					" + System.lineSeparator() +
							"             END_TM = ORG.END_TM,																						" + System.lineSeparator() +
							"             UPD_DT = SYSDATE																							" + System.lineSeparator() +
							"WHEN NOT MATCHED THEN 																									" + System.lineSeparator() +
							"  INSERT(BDATE, CENTER_CD, EQUIP_ID, CENTER_NM, EQUIP_NM, ORD, HEAP_PLAN_INV, 											" + System.lineSeparator() +
							"         HEAP_INV, PLAN_INV, INV, WORKABLE_INV, MULTI_REST_INV, REG_DT, UPD_DT) 										" + System.lineSeparator() +
							"  VALUES(ORG.BDATE, ORG.CENTER_CD, ORG.EQUIP_ID, ORG.CENTER_NM, ORG.EQUIP_NM, ORG.ORD, ORG.HEAP_PLAN_INV, 				" + System.lineSeparator() +
							"         ORG.HEAP_INV, ORG.PLAN_INV, ORG.INV, ORG.WORKABLE_INV, ORG.MULTI_REST_INV, SYSDATE, SYSDATE)";
			
				stmt.execute(sql);
			}
			
			logger.info("Extracted " + list.size() + " Datas");
		}catch(Exception e) {
			//e.printStackTrace();
			logger.log(Level.SEVERE, e.getMessage(), e);
		}finally {
			if(stmt != null) {
				try { stmt.close(); }catch(Exception e) {}
			}
		}
	}
}
