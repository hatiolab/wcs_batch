package com.pluspro.ctrlwcs.extractor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.pluspro.ctrlwcs.beans.EquipmentResultVo;
import com.pluspro.ctrlwcs.util.LogUtil;
import com.pluspro.ctrlwcs.util.SqlUtil;
import com.pluspro.ctrlwcs.util.StringJoiner;

public class WMSResult implements IExtractor {

	Logger logger = LogUtil.getInstance();

	String yyyymmdd;
	Connection orgCon;
	Connection trgCon;

	public WMSResult(String yyyymmdd, Connection orgCon, Connection trgCon) {
		this.yyyymmdd = yyyymmdd;
		this.orgCon = orgCon;
		this.trgCon = trgCon;
	}

	@Override
	public void extract() {
		logger.info("Start to extract WMS");

		ArrayList<EquipmentResultVo> list = extractOrg(this.yyyymmdd);
		list.addAll(extractOrg(SqlUtil.getPreDate(this.yyyymmdd)));
		
		insertTrg(list);

		logger.info("End to extract WMS");
	}

	private ArrayList<EquipmentResultVo> extractOrg(String yyyymmdd) {
		ArrayList<EquipmentResultVo> list = new ArrayList<>();

		Statement stmt = null;
		ResultSet rs = null;

		StringJoiner sql = new StringJoiner(System.lineSeparator());
		sql.add("WITH RSLT AS (");
		sql.add(" SELECT");
		sql.add(" 	REPLACE(B_DATE, '-','') AS BDATE,");
		sql.add(" 	'DT' AS CENTER_CD,");
		sql.add(" 	CENTER_NM,");
		sql.add(" 	CASE ");
		sql.add(" 		WHEN CENTER_NM = '상온' THEN 'NST1'");
		sql.add(" 		WHEN CENTER_NM = '저온' THEN 'NST2'");
		sql.add(" 		WHEN CENTER_NM = '코레일상온' THEN 'NST3'");
		sql.add(" 	ELSE EQUIP_ID");
		sql.add(" 	END AS EQUIP_ID,");
		sql.add(" 	EQUIP_NM,");
		sql.add(" 	ORD,");
		sql.add(" 	PLAN_CUST_CNT AS CUST_CNT,");
		sql.add(" 	CUST_CNT AS CUST,");
		sql.add(" 	PLAN_BOX,");
		sql.add(" 	BOX,");
		sql.add(" 	PLAN_SKU,");
		sql.add(" 	SKU");
		sql.add(" FROM INTERFACE1.DBO.V_DT_MCC_NON_FAC WITH (NOLOCK)");
		sql.add(")");
		sql.add("SELECT * FROM RSLT ");
		sql.add("WHERE BDATE = '" + yyyymmdd + "'");

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
				vo.setCustCnt(rs.getInt("CUST_CNT"));
				vo.setCust(rs.getInt("CUST"));
				vo.setPlanBox(rs.getInt("PLAN_BOX"));
				vo.setBox(rs.getInt("BOX"));
				vo.setPlanSku(rs.getInt("PLAN_SKU"));
				vo.setSku(rs.getInt("SKU"));
//				vo.setStartTm(rs.getTimestamp("START_TM"));
//				vo.setEndTm(rs.getTimestamp("END_TM"));

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

//			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");

			stmt = trgCon.createStatement();

			for (EquipmentResultVo vo : list) {
//				String startTmSql = null;
//				String endTmSql = null;
//
//				if (vo.getStartTm() != null)
//					startTmSql = "TO_DATE('" + sdf.format(vo.getStartTm()) + "', 'yyyymmdd hh24miss')";
//				else
//					startTmSql = "NULL";
//
//				if (vo.getEndTm() != null)
//					endTmSql = "TO_DATE('" + sdf.format(vo.getEndTm()) + "', 'yyyymmdd hh24miss')";
//				else
//					endTmSql = "NULL";

				String sql = "MERGE INTO TB_EQP_RSLT" + System.lineSeparator() +
						"USING(																													" + System.lineSeparator() +
						"    SELECT '" + vo.getBdate() + "' BDATE, '" + vo.getCenterCd() + "'CENTER_CD, '" + vo.getEquipId() + "' EQUIP_ID,		" + System.lineSeparator() +
						"           '" + vo.getCenterNm() + "' CENTER_NM, '" + vo.getEquipNm() + "' EQUIP_NM, '" + vo.getOrd() + "' ORD,		" + System.lineSeparator() +
						"           " + vo.getCustCnt() + " CUST_CNT, " + vo.getCust() + " CUST,												" + System.lineSeparator() +
						"           " + vo.getPlanBox() + " PLAN_BOX, " + vo.getBox() + " BOX,													" + System.lineSeparator() +
//						"           " + startTmSql + " START_TM, " + endTmSql + " END_TM,														" + System.lineSeparator() +
						"           " + vo.getPlanSku() + " PLAN_SKU, " + vo.getSku() + " SKU													" + System.lineSeparator() +
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
//						"             START_TM = ORG.START_TM,																					" + System.lineSeparator() +
//						"             END_TM = ORG.END_TM,																						" + System.lineSeparator() +
						"             UPD_DT = SYSDATE																							" + System.lineSeparator() +
						"WHEN NOT MATCHED THEN 																									" + System.lineSeparator() +
						"  INSERT(BDATE, CENTER_CD, EQUIP_ID, CENTER_NM, EQUIP_NM, ORD, 														" + System.lineSeparator() +
						"         CUST_CNT, CUST, PLAN_BOX, BOX, PLAN_SKU, SKU, REG_DT, UPD_DT) 												" + System.lineSeparator() +
						"  VALUES(ORG.BDATE, ORG.CENTER_CD, ORG.EQUIP_ID, ORG.CENTER_NM, ORG.EQUIP_NM, ORG.ORD, 								" + System.lineSeparator() +
						"         ORG.CUST_CNT, ORG.CUST, ORG.PLAN_BOX, ORG.BOX, ORG.PLAN_SKU, ORG.SKU, SYSDATE, SYSDATE)";

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