package com.pluspro.ctrlwcs.extractor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.StringJoiner;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.pluspro.ctrlwcs.beans.EquipmentResultVo;
import com.pluspro.ctrlwcs.util.LogUtil;
import com.pluspro.ctrlwcs.util.SqlUtil;

public class MPS3Result implements IExtractor {

	Logger logger = LogUtil.getInstance();

	String yyyymmdd;
	Connection orgCon;
	Connection trgCon;

	public MPS3Result(String yyyymmdd, Connection orgCon, Connection trgCon) {
		this.yyyymmdd = yyyymmdd;
		this.orgCon = orgCon;
		this.trgCon = trgCon;
	}

	@Override
	public void extract() {
		logger.info("Start to extract MPS3");

		ArrayList<EquipmentResultVo> list = extractOrg(this.yyyymmdd);
		list.addAll(extractOrg(SqlUtil.preDate(this.yyyymmdd)));

		insertTrg(list);

		logger.info("End to extract MPS3");
	}

	private ArrayList<EquipmentResultVo> extractOrg(String yyyymmdd) {
		ArrayList<EquipmentResultVo> list = new ArrayList<>();

		Statement stmt = null;
		ResultSet rs = null;

		StringJoiner sql = new StringJoiner(System.lineSeparator());
		sql.add("SELECT ");
		sql.add("	   TWSBS.JOB_DATE BDATE,");
		sql.add("       'DT' CENTER_CD,");
		sql.add("       TWSBS.EQUIP_ID EQUIP_ID,");
		sql.add("       '동탄' CENTER_NM,");
		sql.add("       TWSBS.DC_NM || TWSBS.EQUIP_NM EQUIP_NM,");
		sql.add("       TWSBS.DC_CD || '_' || TWSBS.JOB_BATCH_SEQ ORD,");
		sql.add("       TWSBS.PLAN_BOX, ACTUAL_BOX BOX,");
		sql.add("       TWSBS.PLAN_PCS, ACTUAL_PCS PCS,");
		sql.add("       TWSBS.PLAN_SKU, ACTUAL_SKU SKU,       ");
		sql.add("       NVL(TWSBS.COM_CD, '*') COM_CD,");
		sql.add("       TWSBS.CUST_CNT,");
		sql.add("       (");
		sql.add("	       	SELECT COUNT(1) FROM TB_LOCATION ");
		sql.add("	       	WHERE REGION_CD = EQUIP_ID");
		sql.add("	       	AND JOB_STATUS IN ('END', 'ENDED')");
		sql.add("       )  AS CUST,");
		sql.add("       	TJB.INSTRUCTED_AT AS START_TM,");
		sql.add("       	TJB.FINISHED_AT AS END_TM");
		sql.add("FROM TB_WORK_STATUS_BY_SITE TWSBS");
		sql.add("LEFT JOIN (");
		sql.add("		SELECT TB_JOB_BATCH.*, TO_CHAR(TO_DATE(JOB_DATE, 'YYYY-MM-DD'), 'YYYYMMDD') AS BDATE");
		sql.add("		FROM TB_JOB_BATCH ");
		sql.add("	) TJB ");
		sql.add("ON TWSBS.JOB_DATE = TJB.BDATE");
		sql.add("AND TWSBS.EQUIP_ID = TJB.REGION_CD");
		sql.add("AND TWSBS.DC_CD = TJB.DC_CD");
		sql.add("AND TWSBS.JOB_BATCH_SEQ = TJB.JOB_BATCH_SEQ");
		sql.add("WHERE 1 = 1 ");
		sql.add("AND TWSBS.DOMAIN_ID IN (11, 12) ");
		sql.add("AND (TWSBS.JOB_DATE = '" + yyyymmdd + "')");
		sql.add("ORDER BY EQUIP_ID");

		try {
			stmt = orgCon.createStatement();
			rs = stmt.executeQuery(sql.toString());

			while (rs.next()) {
				EquipmentResultVo vo = new EquipmentResultVo();
				vo.setBdate(rs.getString("BDATE"));
				vo.setCenterCd(rs.getString("CENTER_CD"));
				vo.setEquipId(rs.getString("EQUIP_ID"));
				vo.setCenterNm(rs.getString("CENTER_NM"));
				vo.setEquipNm(rs.getString("EQUIP_NM"));
				vo.setOrd(rs.getString("ORD"));
				vo.setComCd(rs.getString("COM_CD"));
				vo.setCustCnt(rs.getInt("CUST_CNT"));
				vo.setPlanBox(rs.getInt("PLAN_BOX"));
				vo.setBox(rs.getInt("BOX"));
				vo.setPlanPcs(rs.getInt("PLAN_PCS"));
				vo.setPcs(rs.getInt("PCS"));
				vo.setPlanSku(rs.getInt("PLAN_SKU"));
				vo.setSku(rs.getInt("SKU"));
				vo.setCust(rs.getInt("CUST"));
				vo.setStartTm(rs.getTimestamp("START_TM"));
				vo.setEndTm(rs.getTimestamp("END_TM"));

				// logger.info(vo.getBdate() + " : " + vo.getCenterCd() + " : " + vo.getEquipId() + " : " + vo.getChuteNo() + " : " + vo.getStatus());

				list.add(vo);
			}

		} catch (Exception e) {
			// e.printStackTrace();
			logger.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {}
			}

			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception e) {}
			}
		}

		return list;
	}

	private void insertTrg(ArrayList<EquipmentResultVo> list) {
		Statement stmt = null;

		try {

			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");

			stmt = trgCon.createStatement();

			for (EquipmentResultVo vo : list) {
				String startTmSql = null;
				String endTmSql = null;

				if (vo.getStartTm() != null)
					startTmSql = "TO_DATE('" + sdf.format(vo.getStartTm()) + "', 'yyyymmdd hh24miss')";
				else
					startTmSql = "NULL";

				if (vo.getEndTm() != null)
					endTmSql = "TO_DATE('" + sdf.format(vo.getEndTm()) + "', 'yyyymmdd hh24miss')";
				else
					endTmSql = "NULL";

				String sql = "MERGE INTO TB_EQP_RSLT	" + System.lineSeparator() +
						"USING(					" + System.lineSeparator() +
						"    SELECT '" + vo.getBdate() + "' BDATE, '" + vo.getCenterCd() + "'CENTER_CD, '" + vo.getEquipId() + "' EQUIP_ID,		" + System.lineSeparator() +
						"           '" + vo.getCenterNm() + "' CENTER_NM, '" + vo.getEquipNm() + "' EQUIP_NM, '" + vo.getOrd() + "' ORD,		" + System.lineSeparator() +
						"           '" + vo.getComCd() + "' COM_CD, " + vo.getCustCnt() + " CUST_CNT,											" + System.lineSeparator() +
						"           " + vo.getPlanBox() + " PLAN_BOX, " + vo.getBox() + " BOX,													" + System.lineSeparator() +
						"           " + vo.getPlanPcs() + " PLAN_PCS, " + vo.getPcs() + " PCS,													" + System.lineSeparator() +
						"           " + vo.getPlanSku() + " PLAN_SKU, " + vo.getSku() + " SKU,													" + System.lineSeparator() +
						"           " + vo.getCust() + " CUST,																					" + System.lineSeparator() +
						"           " + startTmSql + " START_TM, " + endTmSql + " END_TM														" + System.lineSeparator() +
						"      FROM DUAL																										" + System.lineSeparator() +
						") ORG 																													" + System.lineSeparator() +
						"ON (TB_EQP_RSLT.BDATE = ORG.BDATE AND TB_EQP_RSLT.CENTER_CD = ORG.CENTER_CD AND 										" + System.lineSeparator() +
						"    TB_EQP_RSLT.EQUIP_ID = ORG.EQUIP_ID AND TB_EQP_RSLT.ORD = ORG.ORD AND TB_EQP_RSLT.COM_CD = ORG.COM_CD) 			" + System.lineSeparator() +
						"WHEN MATCHED THEN 																										" + System.lineSeparator() +
						"  UPDATE SET CUST_CNT = ORG.CUST_CNT,																					" + System.lineSeparator() +
						"             CUST = ORG.CUST,																							" + System.lineSeparator() +
						"             PLAN_BOX = ORG.PLAN_BOX,																					" + System.lineSeparator() +
						"             BOX = ORG.BOX,																							" + System.lineSeparator() +
						"             PLAN_SKU = ORG.PLAN_SKU,																					" + System.lineSeparator() +
						"             SKU = ORG.SKU,																							" + System.lineSeparator() +
						"             PLAN_PCS = ORG.PLAN_PCS,																					" + System.lineSeparator() +
						"             PCS = ORG.PCS,																							" + System.lineSeparator() +
						"             START_TM = ORG.START_TM,																					" + System.lineSeparator() +
						"             END_TM = ORG.END_TM,																						" + System.lineSeparator() +
						"             UPD_DT = SYSDATE																							" + System.lineSeparator() +
						"WHEN NOT MATCHED THEN 																									" + System.lineSeparator() +
						"  INSERT(BDATE, CENTER_CD, EQUIP_ID, CENTER_NM, EQUIP_NM, ORD, COM_CD, CUST_CNT, 										" + System.lineSeparator() +
						"         PLAN_BOX, BOX, PLAN_PCS, PCS, PLAN_SKU, SKU, CUST, START_TM, END_TM, REG_DT, UPD_DT) 							" + System.lineSeparator() +
						"  VALUES(ORG.BDATE, ORG.CENTER_CD, ORG.EQUIP_ID, ORG.CENTER_NM, ORG.EQUIP_NM, ORG.ORD, ORG.COM_CD, ORG.CUST_CNT, 		" + System.lineSeparator() +
						"         ORG.PLAN_BOX, ORG.BOX, ORG.PLAN_PCS, ORG.PCS, ORG.PLAN_SKU, ORG.SKU, ORG.CUST, ORG.START_TM, ORG.END_TM, SYSDATE, SYSDATE)";

				stmt.execute(sql);
			}

			logger.info("Extracted " + list.size() + " Datas");
		} catch (Exception e) {
			// e.printStackTrace();
			logger.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception e) {}
			}
		}

	}
}
