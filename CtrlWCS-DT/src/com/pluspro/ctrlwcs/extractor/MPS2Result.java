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

public class MPS2Result implements IExtractor {

	public static String CMD_KOR = "KOR"; // �ڷ���
	public static String CMD_BOX = "BOX"; // BOX
	public static String CMD_SUB = "SUB"; // �Һ�(�ܼ�)

	Logger logger = LogUtil.getInstance();

	String cmd;
	String yyyymmdd;

	Connection mps2KorailCon = null; // MPS2.0 �ڷ��� DB Connection
	Connection mps2EEDBoxCon = null; // MPS2.0 EED �Һ� DB Connection
	Connection mps2EEDSubdivisionCon = null; // MPS2.0 EED �ڽ� DB Connection
	Connection trgCon = null; // WCS ���հ��� DB Connection

	public MPS2Result(String cmd, String yyyymmdd, Connection mps2KorailCon, Connection mps2EEDBoxCon, Connection mps2EEDSubdivisionCon, Connection trgCon) {
		this.cmd = cmd;
		this.yyyymmdd = yyyymmdd;
		this.mps2KorailCon = mps2KorailCon;
		this.mps2EEDBoxCon = mps2EEDBoxCon;
		this.mps2EEDSubdivisionCon = mps2EEDSubdivisionCon;
		this.trgCon = trgCon;
	}

	@Override
	public void extract() {
		logger.info("Start to extract MPS2 " + cmd);

		ArrayList<EquipmentResultVo> list = new ArrayList<>();

		if (CMD_KOR.equals(cmd))
			list.addAll(extractKorail());
		if (CMD_BOX.equals(cmd))
			list.addAll(extractEEDBox());
		if (CMD_SUB.equals(cmd))
			list.addAll(extractEEDSubdivision());

		insertTrg(list);

		logger.info("End to extract MPS2" + cmd);
	}

	private ArrayList<EquipmentResultVo> extractKorail() {
		ArrayList<EquipmentResultVo> list = new ArrayList<>();

		Statement stmt = null;
		ResultSet rs = null;

		StringJoiner sql = new StringJoiner(System.lineSeparator());
		sql.add("SELECT BDATE,");
		sql.add("       'DT' CENTER_CD,");
		sql.add("       CENTER_CD + '_' + EQUIP_ID EQUIP_ID,");
		sql.add("       '동탄' CENTER_NM,");
		sql.add("       CENTER_NM + ' ' + EQUIP_NM EQUIP_NM,");
		sql.add("       CONVERT(VARCHAR(50), ORD) ORD,");
		sql.add("       CUST_CNT, CUST,");
		sql.add("       CASE WHEN PLAN_BOX IS NULL THEN 0 ELSE PLAN_BOX END AS PLAN_BOX, BOX,");
		sql.add("       PLAN_PCS, PCS,");
		sql.add("       PLAN_SKU, SKU,");
		sql.add("       STARTTIME, ENDTIME");
		sql.add("  FROM VW_WS_HI_0314_DA1_WCS_TOTAL WITH (NOLOCK)");

		try {
			stmt = mps2KorailCon.createStatement();
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
				vo.setStartTm(rs.getTimestamp("STARTTIME"));
				vo.setEndTm(rs.getTimestamp("ENDTIME"));

				// logger.info(vo.getBdate() + " : " + vo.getCenterCd() + " : " + vo.getEquipId() + " : " + vo.getChuteNo() + " : " + vo.getStatus());

				list.add(vo);
			}

		} catch (Exception e) {
			e.printStackTrace();
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

	private ArrayList<EquipmentResultVo> extractEEDBox() {
		ArrayList<EquipmentResultVo> list = new ArrayList<>();

		Statement stmt = null;
		ResultSet rs = null;

		StringJoiner sql = new StringJoiner(System.lineSeparator());
		sql.add("SELECT BDATE,");
		sql.add("       'DT' CENTER_CD,");
		sql.add("       CENTER_CD + '_' + EQUIP_ID EQUIP_ID,");
		sql.add("       '동탄' CENTER_NM,");
		sql.add("       CENTER_NM + ' ' + EQUIP_NM EQUIP_NM,");
		sql.add("       CONVERT(VARCHAR(50), ORD) ORD,");
		sql.add("       CUST_CNT, CUST,");
		sql.add("       CASE WHEN PLAN_BOX IS NULL THEN 0 ELSE PLAN_BOX END AS PLAN_BOX, BOX,");
		sql.add("       PLAN_PCS, PCS,");
		sql.add("       PLAN_SKU, SKU,");
		sql.add("       STARTTIME, ENDTIME");
		sql.add("FROM VW_DP_E7_9945_DAB_WCS_TOTAL WITH (NOLOCK)");

		try {
			stmt = mps2EEDBoxCon.createStatement();
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
				vo.setStartTm(rs.getTimestamp("STARTTIME"));
				vo.setEndTm(rs.getTimestamp("ENDTIME"));

				// logger.info(vo.getBdate() + " : " + vo.getCenterCd() + " : " + vo.getEquipId() + " : " + vo.getChuteNo() + " : " + vo.getStatus());

				list.add(vo);
			}

		} catch (Exception e) {
			e.printStackTrace();
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

	private ArrayList<EquipmentResultVo> extractEEDSubdivision() {
		ArrayList<EquipmentResultVo> list = new ArrayList<>();

		Statement stmt = null;
		ResultSet rs = null;

		StringJoiner sql = new StringJoiner(System.lineSeparator());
		sql.add("SELECT BDATE,");
		sql.add("       'DT' CENTER_CD,");
		sql.add("       CENTER_CD + '_' + EQUIP_ID EQUIP_ID,");
		sql.add("       '동탄' CENTER_NM,");
		sql.add("       CENTER_NM + ' ' + EQUIP_NM EQUIP_NM,");
		sql.add("       CONVERT(VARCHAR(50), ORD) ORD,");
		sql.add("       CUST_CNT, CUST,");
		sql.add("       CASE WHEN PLAN_BOX IS NULL THEN 0 ELSE PLAN_BOX END AS PLAN_BOX, BOX,");
		sql.add("       PLAN_PCS, PCS,");
		sql.add("       PLAN_SKU, SKU,");
		sql.add("       STARTTIME, ENDTIME");
		sql.add("FROM VW_DP_E7_9945_DAS_WCS_TOTAL WITH (NOLOCK)");

		try {
			stmt = mps2EEDSubdivisionCon.createStatement();
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
				vo.setStartTm(rs.getTimestamp("STARTTIME"));
				vo.setEndTm(rs.getTimestamp("ENDTIME"));

				// logger.info(vo.getBdate() + " : " + vo.getCenterCd() + " : " + vo.getEquipId() + " : " + vo.getChuteNo() + " : " + vo.getStatus());

				list.add(vo);
			}

		} catch (Exception e) {
			e.printStackTrace();
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
						"         PLAN_BOX, BOX, PLAN_PCS, PCS, PLAN_SKU, SKU, START_TM, END_TM, REG_DT, UPD_DT) 								" + System.lineSeparator() +
						"  VALUES(ORG.BDATE, ORG.CENTER_CD, ORG.EQUIP_ID, ORG.CENTER_NM, ORG.EQUIP_NM, ORG.ORD, ORG.CUST_CNT, ORG.CUST,				" + System.lineSeparator() +
						"         ORG.PLAN_BOX, ORG.BOX, ORG.PLAN_PCS, ORG.PCS, ORG.PLAN_SKU, ORG.SKU, ORG.START_TM, ORG.END_TM, SYSDATE, SYSDATE)";

				stmt.execute(sql);

				// logger.info("\n-----------------------------------------------------------\n" + sql);
			}

			logger.info("Extracted " + list.size() + " Datas");
		} catch (Exception e) {
			e.printStackTrace();
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
