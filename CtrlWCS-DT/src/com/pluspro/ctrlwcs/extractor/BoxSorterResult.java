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
import com.pluspro.ctrlwcs.util.SqlUtil;
import com.pluspro.ctrlwcs.util.StringJoiner;

public class BoxSorterResult implements IExtractor {

	Logger logger = LogUtil.getInstance();

	String yyyymmdd;
	Connection orgCon;
	Connection trgCon;

	public BoxSorterResult(String yyyymmdd, Connection orgCon, Connection trgCon) {
		this.yyyymmdd = yyyymmdd;
		this.orgCon = orgCon;
		this.trgCon = trgCon;
	}

	@Override
	public void extract() {
		logger.info("Start to extract BOX ����");

		ArrayList<EquipmentResultVo> list = extractOrg(this.yyyymmdd);
		list.addAll(extractOrg(SqlUtil.getPreDate(this.yyyymmdd)));
		
		insertTrg(list);

		logger.info("End to extract BOX �ڽ� ����");
	}

	private ArrayList<EquipmentResultVo> extractOrg(String yyyymmdd) {
		ArrayList<EquipmentResultVo> list = new ArrayList<>();

		Statement stmt = null;
		ResultSet rs = null;

		/*
		 * 1) �ڽ����� : DS01
		 * 2) ���¸��μ��� : DS02
		 * 3) �������̺긮����� : DS03
		 */

		StringJoiner sql = new StringJoiner(System.lineSeparator());
		sql.add("SELECT B.*, ");
		sql.add("       (SELECT MAX(WRK_STRT_DT) FROM TB_WCS_WRK_BTCH WHERE EQP_ID = B.EQUIP_ID AND BTCH_SEQ = B.BTCH_SEQ) START_TM,");
		sql.add("       (SELECT MAX(WRK_CMPT_DT) FROM TB_WCS_WRK_BTCH WHERE EQP_ID = B.EQUIP_ID AND BTCH_SEQ = B.BTCH_SEQ) END_TM");
		sql.add("  FROM (");
		sql.add("        SELECT RSLT.BDATE,");
		sql.add("               RSLT.CENTER_CD,");
		sql.add("               RSLT.EQUIP_ID,");
		sql.add("               (SELECT COM_DETAIL_NM FROM TB_COMM_CODE_MST WHERE COM_HEAD_CD = 'CENTER_CD' AND COM_DETAIL_CD = RSLT.CENTER_CD) CENTER_NM,");
		sql.add("               (SELECT EQP_NM FROM TB_COMM_EQUIP_MST WHERE EQP_ID = RSLT.EQUIP_ID) EQUIP_NM,");
		sql.add("               RSLT.ORD,");
		sql.add("               MAX(RSLT.BTCH_SEQ) BTCH_SEQ,");
		sql.add("               COUNT(DISTINCT RSLT.BIZPTNR_CD) CUST_CNT,");
		sql.add("               COUNT(DISTINCT RSLT.BIZPTNR_CD) - COUNT(DISTINCT DECODE(RSLT.PLAN_PCS, RSLT.PCS, NULL, RSLT.BIZPTNR_CD)) CUST,");
		sql.add("               SUM(RSLT.PLAN_BOX) PLAN_BOX,");
		sql.add("               SUM(RSLT.BOX) BOX,");
		sql.add("               SUM(RSLT.PLAN_PCS) PLAN_PCS,");
		sql.add("               SUM(RSLT.PCS) PCS,");
		sql.add("               COUNT(DISTINCT RSLT.SKU_CD) PLAN_SKU,");
		sql.add("               COUNT(DISTINCT RSLT.SKU_CD) - COUNT(DISTINCT DECODE(RSLT.PLAN_PCS, RSLT.PCS, NULL, RSLT.SKU_CD)) SKU");
		sql.add("		  FROM(SELECT ");
		sql.add("		        TB_WCS_ORD_HDR.WRK_IDCT_YMD BDATE,         ");
		sql.add("		        TB_WCS_ORD_HDR.CENTER_CD CENTER_CD,");
		sql.add("		        TB_SMS_BOX_SORT_PLAN.EQP_ID EQUIP_ID, ");
		sql.add("		        TB_SMS_BOX_SORT_PLAN.WAV_NO ORD, ");
		sql.add("		        TB_SMS_BOX_SORT_PLAN.BIZPTNR_CD, ");
		sql.add("		        TB_SMS_BOX_SORT_PLAN.SKU_CD,");
		sql.add("		        TB_SMS_BOX_SORT_PLAN.BTCH_SEQ,");
		sql.add("		        SUM(TB_SMS_BOX_SORT_PLAN.PLAN_QTY) PLAN_PCS, ");
		sql.add("		        SUM(TB_SMS_BOX_SORT_PLAN.RSLT_QTY) PCS, ");
		sql.add("		        SUM(TB_SMS_BOX_SORT_PLAN.PLAN_BOX_QTY) PLAN_BOX, ");
		sql.add("		        SUM(TB_SMS_BOX_SORT_PLAN.RSLT_BOX_QTY) BOX ");
		sql.add("			  FROM TB_WCS_ORD_HDR, SORADM.TB_SMS_BOX_SORT_PLAN ");
		sql.add("			  WHERE TB_WCS_ORD_HDR.WRK_IDCT_YMD = '" + yyyymmdd + "'");
		sql.add("			  AND TB_SMS_BOX_SORT_PLAN.EQP_ID IN ('DS01', 'DS02') ");
		sql.add("			  AND TB_WCS_ORD_HDR.CENTER_CD = TB_SMS_BOX_SORT_PLAN.CENTER_CD");
		sql.add("			  AND TB_WCS_ORD_HDR.WAV_NO = TB_SMS_BOX_SORT_PLAN.WAV_NO ");
		sql.add("			  AND TB_WCS_ORD_HDR.ORD_NO = TB_SMS_BOX_SORT_PLAN.ORD_NO ");
		sql.add("			  AND TB_WCS_ORD_HDR.CENTER_CD = 'DT'");
		sql.add("			  GROUP BY TB_WCS_ORD_HDR.WRK_IDCT_YMD, TB_WCS_ORD_HDR.CENTER_CD,");
		sql.add("			  TB_SMS_BOX_SORT_PLAN.EQP_ID, TB_SMS_BOX_SORT_PLAN.WAV_NO,");
		sql.add("			  TB_SMS_BOX_SORT_PLAN.BIZPTNR_CD, TB_SMS_BOX_SORT_PLAN.SKU_CD, ");
		sql.add("			  TB_SMS_BOX_SORT_PLAN.BTCH_SEQ");
		sql.add("			  UNION ALL");
		sql.add("			  SELECT");
		sql.add("					ORD_HDR.WRK_IDCT_YMD BDATE,         ");
		sql.add("					PLAN_DTL.CENTER_CD CENTER_CD,");
		sql.add("					PLAN_DTL.EQP_ID EQUIP_ID, ");
		sql.add("					PLAN_DTL.WAV_NO ORD, ");
		sql.add("					PLAN_HDR.BIZPTNR_CD, ");
		sql.add("					PLAN_DTL.SKU_CD,");
		sql.add("					PLAN_DTL.BTCH_SEQ,");
		sql.add("					SUM(PLAN_DTL.PLAN_QTY) PLAN_PCS, ");
		sql.add("					SUM(PLAN_DTL.RSLT_QTY) PCS,");
		sql.add("					0 PLAN_BOX, ");
		sql.add("					0 BOX ");
		sql.add("				FROM TB_WCS_ORD_HDR ORD_HDR, SORADM.TB_SMS_HBD_SORT_PLAN_HDR PLAN_HDR, SORADM.TB_SMS_HBD_SORT_PLAN_DTL PLAN_DTL");
		sql.add("				WHERE 1 = 1");
		sql.add("				AND ORD_HDR.CENTER_CD = PLAN_DTL.CENTER_CD");
		sql.add("				AND ORD_HDR.WAV_NO = PLAN_DTL.WAV_NO");
		sql.add("				AND ORD_HDR.ORD_NO = PLAN_DTL.ORD_NO");
		sql.add("				AND PLAN_HDR.CENTER_CD = PLAN_DTL.CENTER_CD");
		sql.add("				AND PLAN_HDR.EQP_ID = PLAN_DTL.EQP_ID");
		sql.add("				AND PLAN_HDR.BTCH_SEQ = PLAN_DTL.BTCH_SEQ");
		sql.add("				AND PLAN_HDR.WRK_NO = PLAN_DTL.WRK_NO");
		sql.add("				AND PLAN_DTL.EQP_ID = 'DS03'");
		sql.add("				AND ORD_HDR.WRK_IDCT_YMD = '" + yyyymmdd + "'");
		sql.add("				GROUP BY ");
		sql.add("					ORD_HDR.WRK_IDCT_YMD,");
		sql.add("					PLAN_DTL.CENTER_CD,");
		sql.add("					PLAN_DTL.EQP_ID, ");
		sql.add("					PLAN_DTL.WAV_NO, ");
		sql.add("					PLAN_HDR.BIZPTNR_CD, ");
		sql.add("					PLAN_DTL.SKU_CD,");
		sql.add("					PLAN_DTL.BTCH_SEQ");
		sql.add("			  ) RSLT");
		sql.add("         GROUP BY RSLT.BDATE, RSLT.CENTER_CD, RSLT.EQUIP_ID, RSLT.ORD) B");

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
				vo.setPlanPcs(rs.getInt("PLAN_PCS"));
				vo.setPcs(rs.getInt("PCS"));
				vo.setPlanSku(rs.getInt("PLAN_SKU"));
				vo.setSku(rs.getInt("SKU"));
				vo.setStartTm(rs.getTimestamp("START_TM"));
				vo.setEndTm(rs.getTimestamp("END_TM"));

				// logger.info(vo.getBdate() + " : " + vo.getCenterCd() + " : " + vo.getEquipId() + " : " + vo.getCust() + " : " + vo.getCustCnt());

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

				String sql = "MERGE INTO TB_EQP_RSLT" + System.lineSeparator() +
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
						"  INSERT(BDATE, CENTER_CD, EQUIP_ID, CENTER_NM, EQUIP_NM, ORD, CUST_CNT, CUST,											" + System.lineSeparator() +
						"         PLAN_BOX, BOX, PLAN_PCS, PCS, PLAN_SKU, SKU, REG_DT, UPD_DT) 													" + System.lineSeparator() +
						"  VALUES(ORG.BDATE, ORG.CENTER_CD, ORG.EQUIP_ID, ORG.CENTER_NM, ORG.EQUIP_NM, ORG.ORD, ORG.CUST_CNT, ORG.CUST,			" + System.lineSeparator() +
						"         ORG.PLAN_BOX, ORG.BOX, ORG.PLAN_PCS, ORG.PCS, ORG.PLAN_SKU, ORG.SKU, SYSDATE, SYSDATE)";

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
