package com.pluspro.ctrlwcs.extractor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.pluspro.ctrlwcs.beans.ChuteStatusVo;
import com.pluspro.ctrlwcs.util.LogUtil;
import com.pluspro.ctrlwcs.util.SqlUtil;

public class ChuteStatus implements IExtractor {

	Logger logger = LogUtil.getInstance();

	String yyyymmdd;
	Connection orgCon;
	Connection trgCon;

	public ChuteStatus(String yyyymmdd, Connection orgCon, Connection trgCon) {
		this.yyyymmdd = yyyymmdd;
		this.orgCon = orgCon;
		this.trgCon = trgCon;
	}

	@Override
	public void extract() {

		logger.info("Start to extract ChuteStatus");

		ArrayList<ChuteStatusVo> list = extractOrg(this.yyyymmdd);
		list.addAll(extractOrg(SqlUtil.getPreDate(this.yyyymmdd)));

		insertTrg(list);

		logger.info("Finish to extract ChuteStatus");
	}

	private ArrayList<ChuteStatusVo> extractOrg(String yyyymmdd) {
		ArrayList<ChuteStatusVo> list = new ArrayList<>();

		Statement stmt = null;
		ResultSet rs = null;

		String sql = "SELECT '" + yyyymmdd + "' BDATE, CENTER_CD, EQP_ID,	" + System.lineSeparator() +
				"	     CHUTE_NO, SUBSTRING(STATUS, 1, 1) STATUS			" + System.lineSeparator() +
				"  FROM TB_MCC_IF_EQP_CHUTE WITH (NOLOCK)";

		try {
			stmt = orgCon.createStatement();
			rs = stmt.executeQuery(sql);

			while (rs.next()) {
				ChuteStatusVo vo = new ChuteStatusVo();

				vo.setBdate(rs.getString("BDATE"));
				vo.setCenterCd(rs.getString("CENTER_CD"));
				vo.setEquipId(rs.getString("EQP_ID"));
				vo.setChuteNo(rs.getString("CHUTE_NO"));
				vo.setStatus(rs.getString("STATUS"));

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

	private void insertTrg(ArrayList<ChuteStatusVo> list) {
		Statement stmt = null;

		try {

			stmt = trgCon.createStatement();

			for (ChuteStatusVo vo : list) {
				String sql = "MERGE INTO TB_CHT_STAT                                                                                              " + System.lineSeparator() +
						"USING(                                                                                                              " + System.lineSeparator() +
						"    SELECT '" + vo.getBdate() + "' BDATE, '" + vo.getCenterCd() + "' CENTER_CD, '" + vo.getEquipId() + "' EQP_ID,   " + System.lineSeparator() +
						"           '" + vo.getChuteNo() + "' CHUTE_NO, '" + vo.getStatus() + "' STATUS FROM DUAL                            " + System.lineSeparator() +
						") STAT                                                                                                              " + System.lineSeparator() +
						"ON (TB_CHT_STAT.BDATE = STAT.BDATE AND TB_CHT_STAT.CENTER_CD = STAT.CENTER_CD                                       " + System.lineSeparator() +
						"    AND TB_CHT_STAT.EQUIP_ID = STAT.EQP_ID AND TB_CHT_STAT.CHUTE_NO = STAT.CHUTE_NO)                                  " + System.lineSeparator() +
						"WHEN MATCHED THEN                                                                                                   " + System.lineSeparator() +
						"     UPDATE SET STATUS = STAT.STATUS,                                                                               " + System.lineSeparator() +
						"                UPD_DT = SYSDATE                                                                                    " + System.lineSeparator() +
						"WHEN NOT MATCHED THEN                                                                                               " + System.lineSeparator() +
						"     INSERT(BDATE, CENTER_CD, EQUIP_ID, CHUTE_NO, STATUS, REG_DT, UPD_DT)                                             " + System.lineSeparator() +
						"     VALUES(STAT.BDATE, STAT.CENTER_CD, STAT.EQP_ID, STAT.CHUTE_NO, STAT.STATUS, SYSDATE, SYSDATE)";

				stmt.execute(sql);
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