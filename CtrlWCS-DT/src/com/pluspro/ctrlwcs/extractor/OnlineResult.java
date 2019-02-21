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

public class OnlineResult implements IExtractor {

	Logger logger = LogUtil.getInstance();
	
	String yyyymmdd;
	Connection orgCon;
	Connection trgCon;
	
	public OnlineResult(String yyyymmdd, Connection orgCon, Connection trgCon) {
		this.yyyymmdd = yyyymmdd;
		this.orgCon = orgCon;
		this.trgCon = trgCon;
	}
	
	@Override
	public void extract() {
		logger.info("Start to extract 상온 온라인");
		
		ArrayList<EquipmentResultVo> list = extractOrg();
		insertTrg(list);
		
		logger.info("End to extract 상온 온라인");
	}
	
	private ArrayList<EquipmentResultVo> extractOrg(){
		ArrayList<EquipmentResultVo> list = new ArrayList<>();
		
		Statement stmt = null;
		ResultSet rs = null;

		/*
		1) 상온 단포 출고 라인 : DC05
		2) 상온 단수 출고 라인 : DC50
		3) 상온 온라인 패턴 출고 라인 : DC51
		4) 상온 온라인 단수 출고 라인 : DC53
		5) 상온 패턴 출고 라인 : DC54
		6) 저온 패턴 출고 라인 : DC55
		7) 저온 단포 출고라인 : DC57
		*/
		String sql ="SELECT B.*,                                                                                                            " + System.lineSeparator() +
				    "       (SELECT MAX(WRK_STRT_DT) FROM TB_WCS_WRK_BTCH WHERE EQP_ID = B.EQUIP_ID AND BTCH_SEQ = B.BTCH_SEQ) START_TM,    " + System.lineSeparator() +
				    "       (SELECT MAX(WRK_CMPT_DT) FROM TB_WCS_WRK_BTCH WHERE EQP_ID = B.EQUIP_ID AND BTCH_SEQ = B.BTCH_SEQ) END_TM,		" + System.lineSeparator() +
				    "		(SELECT COUNT(DISTINCT A.CUST)																					" + System.lineSeparator() +
			    	"		        FROM (																									" + System.lineSeparator() +
			    	"		              SELECT TB_WCS_ORD_HDR.WRK_IDCT_YMD, TB_WCS_ORD_DTL.EQP_ID,										" + System.lineSeparator() + 
			    	"		                     TB_WCS_ORD_DTL.WAV_NO, TB_WCS_ORD_HDR.BIZPTNR_CD,											" + System.lineSeparator() +
			    	"		                     DECODE(SUM(TB_WCS_ORD_DTL.PLAN_QTY), SUM(TB_WCS_ORD_DTL.RSLT_QTY), TB_WCS_ORD_HDR.BIZPTNR_CD, NULL) CUST" + System.lineSeparator() +
			    	"		                FROM TB_WCS_ORD_DTL,																			" + System.lineSeparator() +
			    	"		                     TB_WCS_ORD_HDR																				" + System.lineSeparator() +
			    	"		               WHERE TB_WCS_ORD_HDR.WRK_IDCT_YMD = B.BDATE														" + System.lineSeparator() +
			    	"		                 AND TB_WCS_ORD_DTL.EQP_ID = B.EQUIP_ID															" + System.lineSeparator() +
			    	"		                 AND TB_WCS_ORD_HDR.WAV_NO = B.ORD																" + System.lineSeparator() +
			    	"		                 AND TB_WCS_ORD_DTL.WH_ID = TB_WCS_ORD_HDR.WH_ID												" + System.lineSeparator() +
			    	"		                 AND TB_WCS_ORD_DTL.CENTER_CD = TB_WCS_ORD_HDR.CENTER_CD										" + System.lineSeparator() +
			    	"		                 AND TB_WCS_ORD_DTL.WAV_NO = TB_WCS_ORD_HDR.WAV_NO												" + System.lineSeparator() +
			    	"		                 AND TB_WCS_ORD_DTL.ORD_NO = TB_WCS_ORD_HDR.ORD_NO												" + System.lineSeparator() +
			    	"		               GROUP BY TB_WCS_ORD_HDR.WRK_IDCT_YMD, TB_WCS_ORD_DTL.EQP_ID, 									" + System.lineSeparator() +
			    	"		                        TB_WCS_ORD_DTL.WAV_NO, TB_WCS_ORD_HDR.BIZPTNR_CD) A										" + System.lineSeparator() +
			    	"		  GROUP BY WRK_IDCT_YMD, EQP_ID, WAV_NO) CUST																	" + System.lineSeparator() +
				    "  FROM (																												" + System.lineSeparator() +
				    "       SELECT A.BDATE,																									" + System.lineSeparator() + 
					"              A.CENTER_CD,																								" + System.lineSeparator() +
					"              A.EQUIP_ID,																								" + System.lineSeparator() +
					"              (SELECT COM_DETAIL_NM FROM TB_COMM_CODE_MST																" + System.lineSeparator() + 
					"                 WHERE COM_HEAD_CD = 'CENTER_CD' AND COM_DETAIL_CD = A.CENTER_CD) CENTER_NM,							" + System.lineSeparator() + 
					"              (SELECT EQP_NM FROM TB_COMM_EQUIP_MST																	" + System.lineSeparator() +
					"                WHERE EQP_ID = A.EQUIP_ID) EQUIP_NM,																	" + System.lineSeparator() +
					"              A.ORD,																									" + System.lineSeparator() +
					"              MAX(A.BTCH_SEQ) BTCH_SEQ,                                                                  				" + System.lineSeparator() +
					"              COUNT(DISTINCT A.BIZPTNR_CD) CUST_CNT,																	" + System.lineSeparator() + 
					"              SUM(A.PLAN_BOX) PLAN_BOX,																				" + System.lineSeparator() +
					"              SUM(A.BOX) BOX,																							" + System.lineSeparator() +
					"              SUM(A.PLAN_PCS) PLAN_PCS,																				" + System.lineSeparator() +
					"              SUM(A.PCS) PCS,																							" + System.lineSeparator() +
					"              COUNT(DISTINCT A.SKU_CD) PLAN_SKU,																		" + System.lineSeparator() +
					"              COUNT(DISTINCT A.CMP_SKU_CD) SKU,																		" + System.lineSeparator() +
					"              COUNT(DISTINCT A.ORD_NO) PLAN_INV,																		" + System.lineSeparator() +
					"              COUNT(DISTINCT DECODE(A.PLAN_PCS, A.PCS, A.ORD_NO, NULL)) INV											" + System.lineSeparator() +
					"         FROM (SELECT TB_WCS_ORD_HDR.WRK_IDCT_YMD BDATE,																" + System.lineSeparator() +
					"                      TB_WCS_ORD_HDR.CENTER_CD CENTER_CD,																" + System.lineSeparator() +
					"                      TB_WCS_ORD_DTL.EQP_ID EQUIP_ID,																	" + System.lineSeparator() +
					"                      TB_WCS_ORD_HDR.WAV_NO ORD,																		" + System.lineSeparator() +
					"                      TB_WCS_ORD_HDR.BIZPTNR_CD,																		" + System.lineSeparator() +
					"                      TB_WCS_ORD_DTL.SKU_CD,																			" + System.lineSeparator() +
					"                      TB_WCS_ORD_HDR.ORD_NO,                                                                           " + System.lineSeparator() +
					"                      TB_WCS_ORD_DTL.BTCH_SEQ,                                                                  		" + System.lineSeparator() +
					"	                   DECODE(SUM(TB_WCS_ORD_DTL.PLAN_QTY), SUM(TB_WCS_ORD_DTL.RSLT_QTY), TB_WCS_ORD_DTL.SKU_CD, NULL) CMP_SKU_CD,	" + System.lineSeparator() + 
					"                      SUM(TB_WCS_ORD_DTL.PLAN_QTY) PLAN_PCS,															" + System.lineSeparator() +
					"                      SUM(TB_WCS_ORD_DTL.RSLT_QTY) PCS,																" + System.lineSeparator() +
					"                      SUM(CEIL(TB_WCS_ORD_DTL.PLAN_QTY / TB_WCS_ORD_DTL.BOX_IN_QTY)) PLAN_BOX,							" + System.lineSeparator() + 
					"                      SUM(CEIL(TB_WCS_ORD_DTL.RSLT_QTY / TB_WCS_ORD_DTL.BOX_IN_QTY)) BOX								" + System.lineSeparator() +
					"                 FROM TB_WCS_ORD_DTL,																					" + System.lineSeparator() +
					"                      TB_WCS_ORD_HDR																					" + System.lineSeparator() +
					"                WHERE TB_WCS_ORD_HDR.WRK_IDCT_YMD = '" + yyyymmdd + "'													" + System.lineSeparator() + 
					"                  AND TB_WCS_ORD_DTL.EQP_ID IN ('DC05', 'DC50', 'DC51', 'DC53', 'DC54', 'DC55', 'DC57')				" + System.lineSeparator() +
					"                  AND TB_WCS_ORD_DTL.WH_ID = TB_WCS_ORD_HDR.WH_ID														" + System.lineSeparator() +
					"                  AND TB_WCS_ORD_DTL.CENTER_CD = TB_WCS_ORD_HDR.CENTER_CD												" + System.lineSeparator() +
					"                  AND TB_WCS_ORD_DTL.WAV_NO = TB_WCS_ORD_HDR.WAV_NO													" + System.lineSeparator() +
					"                  AND TB_WCS_ORD_DTL.ORD_NO = TB_WCS_ORD_HDR.ORD_NO													" + System.lineSeparator() +
					"                  AND TB_WCS_ORD_DTL.EQP_ID IS NOT NULL																" + System.lineSeparator() +
					"                GROUP BY TB_WCS_ORD_HDR.WRK_IDCT_YMD, TB_WCS_ORD_HDR.CENTER_CD,										" + System.lineSeparator() +
					"                         TB_WCS_ORD_DTL.EQP_ID, TB_WCS_ORD_HDR.BIZPTNR_CD,												" + System.lineSeparator() +
					"                         TB_WCS_ORD_DTL.SKU_CD, TB_WCS_ORD_HDR.WAV_NO,TB_WCS_ORD_HDR.ORD_NO,TB_WCS_ORD_DTL.BTCH_SEQ) A	" + System.lineSeparator() +
					"        GROUP BY  A.BDATE, A.CENTER_CD, A.EQUIP_ID,																	" + System.lineSeparator() +
					"                  A.CENTER_CD, A.EQUIP_ID, A.ORD) B";
		
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
				vo.setCustCnt(rs.getInt("CUST_CNT"));
				vo.setCust(rs.getInt("CUST"));
				vo.setPlanBox(rs.getInt("PLAN_BOX"));
				vo.setBox(rs.getInt("BOX"));
				vo.setPlanPcs(rs.getInt("PLAN_PCS"));
				vo.setPcs(rs.getInt("PCS"));
				vo.setPlanSku(rs.getInt("PLAN_SKU"));
				vo.setSku(rs.getInt("SKU"));
				vo.setPlanInv(rs.getInt("PLAN_INV"));
				vo.setInv(rs.getInt("INV"));
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
							"           " + vo.getCustCnt() + " CUST_CNT, " + vo.getCust() + " CUST,												" + System.lineSeparator() +
							"           " + vo.getPlanBox() + " PLAN_BOX, " + vo.getBox() + " BOX,													" + System.lineSeparator() +
							"           " + vo.getPlanPcs() + " PLAN_PCS, " + vo.getPcs() + " PCS,													" + System.lineSeparator() +
							"           " + vo.getPlanSku() + " PLAN_SKU, " + vo.getSku() + " SKU,													" + System.lineSeparator() +
							"           " + vo.getPlanInv() + " PLAN_INV, " + vo.getInv() + " INV,													" + System.lineSeparator() +
							"           " + startTmSql + " START_TM, " + endTmSql + " END_TM														" + System.lineSeparator() +
							"      FROM DUAL																										" + System.lineSeparator() +
							") ORG 																													" + System.lineSeparator() +
							"ON (TB_EQP_RSLT.BDATE = ORG.BDATE AND TB_EQP_RSLT.CENTER_CD = ORG.CENTER_CD AND 										" + System.lineSeparator() +
							"    TB_EQP_RSLT.EQUIP_ID = ORG.EQUIP_ID AND TB_EQP_RSLT.ORD = ORG.ORD) 												" + System.lineSeparator() +
							"WHEN MATCHED THEN 																										" + System.lineSeparator() +
							"  UPDATE SET CUST_CNT = ORG.CUST_CNT,																					" + System.lineSeparator() +
							"             CUST = ORG.CUST,																							" + System.lineSeparator() +
							"             PLAN_BOX = ORG.PLAN_BOX,																					" + System.lineSeparator() +
							"             BOX = ORG.BOX,																							" + System.lineSeparator() +
							"             PLAN_SKU = ORG.PLAN_SKU,																					" + System.lineSeparator() +
							"             SKU = ORG.SKU,																							" + System.lineSeparator() +
							"             PLAN_PCS = ORG.PLAN_PCS,																					" + System.lineSeparator() +
							"             PCS = ORG.PCS,																							" + System.lineSeparator() +
							"             PLAN_INV = ORG.PLAN_INV,																					" + System.lineSeparator() +
							"             INV = ORG.INV,																							" + System.lineSeparator() +
							"             PLAN_SKU_INV = ORG.PLAN_SKU,																				" + System.lineSeparator() +
							"             SKU_INV = ORG.SKU,																						" + System.lineSeparator() +
							"             START_TM = ORG.START_TM,																					" + System.lineSeparator() +
							"             END_TM = ORG.END_TM,																						" + System.lineSeparator() +
							"             UPD_DT = SYSDATE																							" + System.lineSeparator() +
							"WHEN NOT MATCHED THEN 																									" + System.lineSeparator() +
							"  INSERT(BDATE, CENTER_CD, EQUIP_ID, CENTER_NM, EQUIP_NM, ORD, CUST_CNT, CUST,											" + System.lineSeparator() +
							"         PLAN_BOX, BOX, PLAN_PCS, PCS, PLAN_SKU, SKU, PLAN_INV, INV, PLAN_SKU_INV, SKU_INV, REG_DT, UPD_DT)			" + System.lineSeparator() +
							"  VALUES(ORG.BDATE, ORG.CENTER_CD, ORG.EQUIP_ID, ORG.CENTER_NM, ORG.EQUIP_NM, ORG.ORD, ORG.CUST_CNT, ORG.CUST,			" + System.lineSeparator() +
							"         ORG.PLAN_BOX, ORG.BOX, ORG.PLAN_PCS, ORG.PCS, ORG.PLAN_SKU, ORG.SKU, ORG.PLAN_INV, ORG.INV, 					" + System.lineSeparator() +
							"         ORG.PLAN_SKU, ORG.SKU, SYSDATE, SYSDATE)";
			
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
