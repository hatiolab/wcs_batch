package com.pluspro.ctrlwcs.extractor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.pluspro.ctrlwcs.beans.ChuteResultVo;
import com.pluspro.ctrlwcs.util.LogUtil;
import com.pluspro.ctrlwcs.util.SqlUtil;
import com.pluspro.ctrlwcs.util.StringJoiner;

public class ChuteResult implements IExtractor {

	Logger logger = LogUtil.getInstance();

	String yyyymmdd;
	Connection chtCon;
	Connection wcsCon;
	Connection trgCon;

	public ChuteResult(String yyyymmdd, Connection chtCon, Connection wcsCon, Connection trgCon) {
		this.yyyymmdd = yyyymmdd;
		this.chtCon = chtCon;
		this.wcsCon = wcsCon;
		this.trgCon = trgCon;
	}

	@Override
	public void extract() {

		logger.info("Start to extract ChuteResult");

		List<String> dateList = Arrays.asList(this.yyyymmdd, SqlUtil.getPreDate(this.yyyymmdd));
		for (String date : dateList) {
			HashMap<String, ChuteResultVo> map = extractOrg(date);
			setProgress(map);
			insertTrg(map);
		}

		logger.info("Finish to extract ChuteResult");
	}

	private HashMap<String, ChuteResultVo> extractOrg(String yyyymmdd) {
		HashMap<String, ChuteResultVo> map = new HashMap<>();

		Statement stmt = null;
		ResultSet rs = null;

		String sql = "SELECT '" + yyyymmdd + "' BDATE, CENTER_CD, EQP_ID, CHUTE_NO" + System.lineSeparator() +
				"  FROM TB_MCC_IF_EQP_CHUTE WITH (NOLOCK)";

		try {
			stmt = chtCon.createStatement();
			rs = stmt.executeQuery(sql);

			while (rs.next()) {

				ChuteResultVo vo = new ChuteResultVo();

				vo.setBdate(rs.getString("BDATE"));
				vo.setCenterCd(rs.getString("CENTER_CD"));
				vo.setEquipId(rs.getString("EQP_ID"));
				vo.setChuteNo(rs.getString("CHUTE_NO"));

				String key = vo.getEquipId() + "_" + vo.getChuteNo();
				map.put(key, vo);

				// logger.info(vo.getBdate() + " : " + vo.getCenterCd() + " : " + vo.getEquipId() + " : " + vo.getChuteNo());
			}

		} catch (Exception e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, "Error", e);
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

		return map;
	}

	private void setProgress(HashMap<String, ChuteResultVo> map) {

		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = wcsCon.createStatement();

			StringJoiner sql = new StringJoiner(System.lineSeparator());
			sql.add("SELECT");
			sql.add("  A.EQP_ID,");
			sql.add("  A.CHUTE_ID,");
			sql.add("  PLAN_QTY,");
			sql.add("  RSLT_QTY,");
			sql.add("  DECODE(PLAN_QTY, 0, 0, FLOOR(RSLT_QTY / PLAN_QTY * 100)) PROGRESS_RT");
			sql.add("FROM");
			sql.add("  	(");
			sql.add("	  SELECT");
			sql.add("	    TB_SMS_BOX_SORT_PLAN.EQP_ID,");
			sql.add("	    TB_SMS_BOX_SORT_PLAN.CHUTE_ID,");
			sql.add("	    SUM(TB_SMS_BOX_SORT_PLAN.PLAN_BOX_QTY) PLAN_QTY,");
			sql.add("	    SUM(TB_SMS_BOX_SORT_PLAN.RSLT_BOX_QTY) RSLT_QTY");
			sql.add("	  FROM");
			sql.add("	    TB_WCS_ORD_HDR,");
			sql.add("	    SORADM.TB_SMS_BOX_SORT_PLAN");
			sql.add("	  WHERE");
			sql.add("	    TB_WCS_ORD_HDR.WRK_IDCT_YMD = '" + yyyymmdd + "'");
			sql.add("	    AND TB_WCS_ORD_HDR.CENTER_CD = TB_SMS_BOX_SORT_PLAN.CENTER_CD");
			sql.add("	    AND TB_WCS_ORD_HDR.WAV_NO = TB_SMS_BOX_SORT_PLAN.WAV_NO");
			sql.add("	    AND TB_WCS_ORD_HDR.ORD_NO = TB_SMS_BOX_SORT_PLAN.ORD_NO");
			sql.add("	    AND TB_WCS_ORD_HDR.CENTER_CD = 'DT'");
			sql.add("	  GROUP BY");
			sql.add("	    EQP_ID, CHUTE_ID");
			sql.add("	  UNION");
			sql.add("	  SELECT");
			sql.add("		PLAN_DTL.EQP_ID,");
			sql.add("		PLAN_HDR.CHUTE_ID,");
			sql.add("		SUM(PLAN_DTL.PLAN_QTY) AS PLAN_QTY,");
			sql.add("		SUM(PLAN_DTL.RSLT_QTY) AS RSLT_QTY");
			sql.add("	  FROM TB_WCS_ORD_HDR ORD_HDR, SORADM.TB_SMS_HBD_SORT_PLAN_HDR PLAN_HDR, SORADM.TB_SMS_HBD_SORT_PLAN_DTL PLAN_DTL");
			sql.add("	  WHERE 1 = 1");
			sql.add("		AND ORD_HDR.CENTER_CD = PLAN_DTL.CENTER_CD");
			sql.add("		AND ORD_HDR.WAV_NO = PLAN_DTL.WAV_NO");
			sql.add("		AND ORD_HDR.ORD_NO = PLAN_DTL.ORD_NO");
			sql.add("		AND PLAN_HDR.CENTER_CD = PLAN_DTL.CENTER_CD");
			sql.add("		AND PLAN_HDR.EQP_ID = PLAN_DTL.EQP_ID");
			sql.add("		AND PLAN_HDR.BTCH_SEQ = PLAN_DTL.BTCH_SEQ");
			sql.add("		AND PLAN_HDR.WRK_NO = PLAN_DTL.WRK_NO");
			sql.add("		AND PLAN_DTL.EQP_ID = 'DS03'");
			sql.add("		AND ORD_HDR.WRK_IDCT_YMD = '" + yyyymmdd + "'");
			sql.add("	  GROUP BY ORD_HDR.WRK_IDCT_YMD, PLAN_DTL.CENTER_CD, PLAN_DTL.EQP_ID, ORD_HDR.WAV_NO, ORD_HDR.ORIGN_WAV_NO, PLAN_HDR.CHUTE_ID");
			sql.add("	) A");

			rs = stmt.executeQuery(sql.toString());

			while (rs.next()) {
				String eqpId = rs.getString("EQP_ID");
				String chuteId = rs.getString("CHUTE_ID");
				int progressRt = rs.getInt("PROGRESS_RT");
				int planQty = rs.getInt("PLAN_QTY");
				int qty = rs.getInt("RSLT_QTY");

				String key = eqpId + "_" + chuteId;
				if (map.containsKey(key)) {
					ChuteResultVo vo = map.get(key);
					vo.setProgressRt(progressRt);
					vo.setPlanQty(planQty);
					vo.setQty(qty);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, "Error", e);
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
	}

	private void insertTrg(HashMap<String, ChuteResultVo> map) {
		Statement stmt = null;

		try {

			stmt = trgCon.createStatement();

			for (ChuteResultVo vo : map.values()) {
				String sql = "MERGE INTO TB_CHT_RSLT                                                                                              	" + System.lineSeparator() +
						"USING(                                                                                                              	" + System.lineSeparator() +
						"    SELECT '" + vo.getBdate() + "' BDATE, '" + vo.getCenterCd() + "' CENTER_CD, '" + vo.getEquipId() + "' EQP_ID,   	" + System.lineSeparator() +
						"           '" + vo.getChuteNo() + "' CHUTE_NO, " + vo.getProgressRt() + " PROGRESS_RT, 								" + System.lineSeparator() +
						"           " + vo.getPlanQty() + " PLAN_QTY, " + vo.getQty() + " QTY 												  	" + System.lineSeparator() +
						"      FROM DUAL                     																					" + System.lineSeparator() +
						") RSLT                                                                                                              	" + System.lineSeparator() +
						"ON (TB_CHT_RSLT.BDATE = RSLT.BDATE AND TB_CHT_RSLT.CENTER_CD = RSLT.CENTER_CD                                       	" + System.lineSeparator() +
						"    AND TB_CHT_RSLT.EQUIP_ID = RSLT.EQP_ID AND TB_CHT_RSLT.CHUTE_NO = RSLT.CHUTE_NO)                                  " + System.lineSeparator() +
						"WHEN MATCHED THEN                                                                                                   	" + System.lineSeparator() +
						"     UPDATE SET PROGRESS_RT = RSLT.PROGRESS_RT,                                                                     	" + System.lineSeparator() +
						"                PLAN_QTY = RSLT.PLAN_QTY,																			    " + System.lineSeparator() +
						"                QTY = RSLT.QTY,																					    " + System.lineSeparator() +
						"                UPD_DT = SYSDATE                                                                                    	" + System.lineSeparator() +
						"WHEN NOT MATCHED THEN                                                                                               	" + System.lineSeparator() +
						"     INSERT(BDATE, CENTER_CD, EQUIP_ID, CHUTE_NO, PROGRESS_RT, PLAN_QTY, QTY, REG_DT, UPD_DT)                         " + System.lineSeparator() +
						"     VALUES(RSLT.BDATE, RSLT.CENTER_CD, RSLT.EQP_ID, RSLT.CHUTE_NO, RSLT.PROGRESS_RT, RSLT.PLAN_QTY, RSLT.QTY, SYSDATE, SYSDATE)";

				stmt.execute(sql);
			}

			logger.info("Extracted " + map.size() + " Datas");
		} catch (Exception e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, "Error", e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception e) {}
			}
		}
	}
}