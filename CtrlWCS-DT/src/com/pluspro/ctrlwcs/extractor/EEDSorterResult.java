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

public class EEDSorterResult implements IExtractor {

Logger logger = LogUtil.getInstance();
	
	String yyyymmdd;
	Connection orgCon;
	Connection trgCon;
	
	String CENTER_CD = "DT";
	String CENTER_NM = "동탄";
	String EQUIP_ID = "EEDS";
	String EQUIP_NM = "EED 소터";
	
	public EEDSorterResult(String yyyymmdd, Connection orgCon, Connection trgCon) {
		this.yyyymmdd = yyyymmdd;
		this.orgCon = orgCon;
		this.trgCon = trgCon;
	}
	
	@Override
	public void extract() {
		logger.info("Start to extract EED 소터");
		
		ArrayList<EquipmentResultVo> list = extractOrg();
		insertTrg(list);
		
		logger.info("End to extract EED 박스 소터");
	}

	private ArrayList<EquipmentResultVo> extractOrg(){
		ArrayList<EquipmentResultVo> list = new ArrayList<>();
		
		list = extractCustCnt(list);
		list = extractSku(list);
		list = extractBox(list);
		list = extractTime(list);
		
		return list;
	}
	
	private ArrayList<EquipmentResultVo> extractCustCnt(ArrayList<EquipmentResultVo> list) {
	
		Statement stmt = null;
		ResultSet rs = null;
		
		String sql ="SELECT BATCHNO ID,													" + System.lineSeparator() +
					"       GI_YMD BDATE,												" + System.lineSeparator() +
					"       WAV_NO ORD,													" + System.lineSeparator() +
					"       COUNT(DISTINCT BIZPTNR_CD) CUST_CNT,						" + System.lineSeparator() +
					"       COUNT(DISTINCT DECODE(TQTY, SQTY, BIZPTNR_CD, NULL)) CUST	" + System.lineSeparator() +
					"  FROM SMS_DT.VW_CUSTSUM											" + System.lineSeparator() +
					" GROUP BY BATCHNO, GI_YMD, WAV_NO";
		
		try {
			stmt = orgCon.createStatement();
			rs = stmt.executeQuery(sql);
			
			while(rs.next()) {
				EquipmentResultVo vo = new EquipmentResultVo();
				vo.setId(rs.getString("ID"));
				vo.setBdate(rs.getString("BDATE"));
				vo.setCenterCd(CENTER_CD);
				vo.setEquipId(EQUIP_ID);
				vo.setCenterNm(CENTER_NM);
				vo.setEquipNm(EQUIP_NM);
				vo.setOrd(rs.getString("ORD"));
				vo.setCustCnt(rs.getInt("CUST_CNT"));
				vo.setCust(rs.getInt("CUST"));
				
				//logger.info(vo.getBdate() + " : " + vo.getCenterCd() + " : " + vo.getEquipId() + " : " + vo.getCust());
				
				boolean isExist = false;
				for(EquipmentResultVo _vo : list) {
					if(vo.getId().equals(_vo.getId()) && vo.getBdate().equals(_vo.getBdate()) && vo.getOrd().equals(_vo.getOrd())) {
						_vo.setCustCnt(vo.getCustCnt());
						
						isExist = true;
						break;
					}
				}
				
				if(isExist == false) list.add(vo);
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
	
	private ArrayList<EquipmentResultVo> extractSku(ArrayList<EquipmentResultVo> list) {
		
		Statement stmt = null;
		ResultSet rs = null;
		
		String sql ="SELECT BATCHNO ID,												" + System.lineSeparator() +
					"       GI_YMD BDATE,											" + System.lineSeparator() +
					"       WAV_NO ORD,												" + System.lineSeparator() +
					"       COUNT(DISTINCT SKU_CD) PLAN_SKU,						" + System.lineSeparator() +
					"       COUNT(DISTINCT DECODE(TQTY, SQTY, SKU_CD, NULL)) SKU	" + System.lineSeparator() +
					"  FROM SMS_DT.VW_SKUSUM										" + System.lineSeparator() +
					" GROUP BY BATCHNO, GI_YMD, WAV_NO";
		
		try {
			stmt = orgCon.createStatement();
			rs = stmt.executeQuery(sql);
			
			while(rs.next()) {
				EquipmentResultVo vo = new EquipmentResultVo();
				vo.setId(rs.getString("ID"));
				vo.setBdate(rs.getString("BDATE"));
				vo.setCenterCd(CENTER_CD);
				vo.setEquipId(EQUIP_ID);
				vo.setCenterNm(CENTER_NM);
				vo.setEquipNm(EQUIP_NM);
				vo.setOrd(rs.getString("ORD"));
				vo.setPlanSku(rs.getInt("PLAN_SKU"));
				vo.setSku(rs.getInt("SKU"));
				
				//logger.info(vo.getBdate() + " : " + vo.getCenterCd() + " : " + vo.getEquipId() + " : " + vo.getChuteNo() + " : " + vo.getStatus());
				
				boolean isExist = false;
				for(EquipmentResultVo _vo : list) {
					if(vo.getId().equals(_vo.getId()) && vo.getBdate().equals(_vo.getBdate()) && vo.getOrd().equals(_vo.getOrd())) {
						_vo.setPlanSku(vo.getPlanSku());
						_vo.setSku(vo.getSku());
						
						isExist = true;
						break;
					}
				}
				
				if(isExist == false) list.add(vo);
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
	
	private ArrayList<EquipmentResultVo> extractBox(ArrayList<EquipmentResultVo> list) {
		
		Statement stmt = null;
		ResultSet rs = null;
		
		String sql ="SELECT BATCHNO ID,			" + System.lineSeparator() +
					"       GI_YMD BDATE,		" + System.lineSeparator() +
					"       WAV_NO ORD,			" + System.lineSeparator() +
					"       TQTY PLAN_BOX,		" + System.lineSeparator() +
					"       SQTY BOX			" + System.lineSeparator() +
					"  FROM SMS_DT.VW_BOXSUM";
		
		try {
			stmt = orgCon.createStatement();
			rs = stmt.executeQuery(sql);
			
			while(rs.next()) {
				EquipmentResultVo vo = new EquipmentResultVo();
				vo.setId(rs.getString("ID"));
				vo.setBdate(rs.getString("BDATE"));
				vo.setCenterCd(CENTER_CD);
				vo.setEquipId(EQUIP_ID);
				vo.setCenterNm(CENTER_NM);
				vo.setEquipNm(EQUIP_NM);
				vo.setOrd(rs.getString("ORD"));
				vo.setPlanBox(rs.getInt("PLAN_BOX"));
				vo.setBox(rs.getInt("BOX"));
				
				logger.info(vo.getBdate() + " : " + vo.getCenterCd() + " : " + vo.getEquipId());
				
				boolean isExist = false;
				for(EquipmentResultVo _vo : list) {
					if(vo.getId().equals(_vo.getId()) && vo.getBdate().equals(_vo.getBdate()) && vo.getOrd().equals(_vo.getOrd())) {
						_vo.setPlanBox(vo.getPlanBox());
						_vo.setBox(vo.getBox());
						
						isExist = true;
						break;
					}
				}
				
				if(isExist == false) list.add(vo);
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
	
	private ArrayList<EquipmentResultVo> extractTime(ArrayList<EquipmentResultVo> list) {
		
		Statement stmt = null;
		ResultSet rs = null;
		
		String sql ="SELECT BATCHNO ID,			" + System.lineSeparator() +
					"       GI_YMD BDATE,		" + System.lineSeparator() +
					"       WAV_NO ORD,			" + System.lineSeparator() +
					"       START_DT START_TM,	" + System.lineSeparator() +
					"       COMPLETE_DT END_TM	" + System.lineSeparator() +
					"  FROM SMS_DT.VW_CHASUINFO";
		
		try {
			stmt = orgCon.createStatement();
			rs = stmt.executeQuery(sql);
			
			while(rs.next()) {
				EquipmentResultVo vo = new EquipmentResultVo();
				vo.setId(rs.getString("ID"));
				vo.setBdate(rs.getString("BDATE"));
				vo.setCenterCd(CENTER_CD);
				vo.setEquipId(EQUIP_ID);
				vo.setCenterNm(CENTER_NM);
				vo.setEquipNm(EQUIP_NM);
				vo.setOrd(rs.getString("ORD"));
				vo.setStartTm(rs.getTimestamp("START_TM"));
				vo.setEndTm(rs.getTimestamp("END_TM"));
				
				logger.info(vo.getBdate() + " : " + vo.getCenterCd() + " : " + vo.getEquipId());
				logger.info(vo.getStartTm() + " : " + vo.getEndTm() + " : " + vo.getEquipId());
				
				boolean isExist = false;
				for(EquipmentResultVo _vo : list) {
					if(vo.getId().equals(_vo.getId()) && vo.getBdate().equals(_vo.getBdate()) && vo.getOrd().equals(_vo.getOrd())) {
						_vo.setStartTm(vo.getStartTm());
						_vo.setEndTm(vo.getEndTm());
						
						isExist = true;
						break;
					}
				}
				
				if(isExist == false) list.add(vo);
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
							"           " + startTmSql + " START_TM, " + endTmSql + " END_TM														" + System.lineSeparator() +
							"      FROM DUAL																										" + System.lineSeparator() +
							") ORG 																													" + System.lineSeparator() +
							"ON (TB_EQP_RSLT.BDATE = ORG.BDATE AND TB_EQP_RSLT.CENTER_CD = ORG.CENTER_CD AND 										" + System.lineSeparator() +
							"    TB_EQP_RSLT.EQUIP_ID = ORG.EQUIP_ID AND TB_EQP_RSLT.ORD = ORG.ORD) 												" + System.lineSeparator() +
							"WHEN MATCHED THEN 																										" + System.lineSeparator() +
							"  UPDATE SET CUST_CNT = DECODE(ORG.CUST_CNT, 0, TB_EQP_RSLT.CUST_CNT, ORG.CUST_CNT),									" + System.lineSeparator() +
							"             CUST = DECODE(ORG.CUST, 0, TB_EQP_RSLT.CUST, ORG.CUST),													" + System.lineSeparator() +
							"             PLAN_BOX = DECODE(ORG.PLAN_BOX, 0, TB_EQP_RSLT.PLAN_BOX, ORG.PLAN_BOX),									" + System.lineSeparator() +
							"             BOX = DECODE(ORG.BOX, 0, TB_EQP_RSLT.BOX, ORG.BOX),														" + System.lineSeparator() +
							"             PLAN_SKU = DECODE(ORG.PLAN_SKU, 0, TB_EQP_RSLT.PLAN_SKU, ORG.PLAN_SKU),									" + System.lineSeparator() +
							"             SKU = DECODE(ORG.SKU,	0, TB_EQP_RSLT.SKU, ORG.SKU),														" + System.lineSeparator() +
							"             PLAN_PCS = DECODE(ORG.PLAN_PCS, 0, TB_EQP_RSLT.PLAN_PCS, ORG.PLAN_PCS), 									" + System.lineSeparator() +
							"             PCS = DECODE(ORG.PCS, 0, TB_EQP_RSLT.PCS, ORG.PCS),														" + System.lineSeparator() +
							"             START_TM = NVL(ORG.START_TM, TB_EQP_RSLT.START_TM),														" + System.lineSeparator() +
							"             END_TM = NVL(ORG.END_TM, TB_EQP_RSLT.END_TM),																" + System.lineSeparator() +
							"             UPD_DT = SYSDATE																							" + System.lineSeparator() +
							"WHEN NOT MATCHED THEN 																									" + System.lineSeparator() +
							"  INSERT(BDATE, CENTER_CD, EQUIP_ID, CENTER_NM, EQUIP_NM, ORD, CUST_CNT, CUST,											" + System.lineSeparator() +
							"         PLAN_BOX, BOX, PLAN_PCS, PCS, PLAN_SKU, SKU, START_TM, END_TM, REG_DT, UPD_DT) 								" + System.lineSeparator() +
							"  VALUES(ORG.BDATE, ORG.CENTER_CD, ORG.EQUIP_ID, ORG.CENTER_NM, ORG.EQUIP_NM, ORG.ORD, ORG.CUST_CNT, ORG.CUST,			" + System.lineSeparator() +
							"         ORG.PLAN_BOX, ORG.BOX, ORG.PLAN_PCS, ORG.PCS, ORG.PLAN_SKU, ORG.SKU, ORG.START_TM, ORG.END_TM, SYSDATE, SYSDATE)";
			
				//logger.info("\n" + sql);
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
