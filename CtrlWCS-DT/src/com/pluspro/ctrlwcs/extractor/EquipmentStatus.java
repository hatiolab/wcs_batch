package com.pluspro.ctrlwcs.extractor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.pluspro.ctrlwcs.beans.EquipmentStatusVo;
import com.pluspro.ctrlwcs.util.LogUtil;
import com.pluspro.ctrlwcs.util.SqlUtil;

public class EquipmentStatus implements IExtractor {

	Logger logger = LogUtil.getInstance();

	String yyyymmdd;
	Connection orgCon;
	Connection trgCon;

	public EquipmentStatus(String yyyymmdd, Connection orgCon, Connection trgCon) {
		this.yyyymmdd = yyyymmdd;
		this.orgCon = orgCon;
		this.trgCon = trgCon;
	}

	@Override
	public void extract() {

		logger.info("Start to extract EquipmentStatus");

		ArrayList<EquipmentStatusVo> list = extractOrg(this.yyyymmdd);
		list.addAll(extractOrg(SqlUtil.getPreDate(this.yyyymmdd)));

		insertTrg(list);

		logger.info("Finish to extract EquipmentStatus");
	}

	private ArrayList<EquipmentStatusVo> extractOrg(String yyyymmdd) {
		ArrayList<EquipmentStatusVo> list = new ArrayList<>();

		Statement stmt = null;
		ResultSet rs = null;

		// CENTER_CD EQUIP_ID EQUIP_NM STATUS ERR_MSG
		String sql = "SELECT '" + yyyymmdd + "' BDATE, CENTER_CD, 	" + System.lineSeparator() +
				"       EQUIP_ID, EQUIP_NM, 						" + System.lineSeparator() +
				"	     SUBSTRING(STATUS, 1, 1) STATUS, ERR_MSG	" + System.lineSeparator() +
				"  FROM TB_MCC_IF_EQP WITH (NOLOCK)";

		try {
			stmt = orgCon.createStatement();
			rs = stmt.executeQuery(sql);

			while (rs.next()) {

				EquipmentStatusVo vo = new EquipmentStatusVo();

				// ChuteStatusVo vo = new ChuteStatusVo();

				vo.setBdate(rs.getString("BDATE"));
				vo.setCenterCd(rs.getString("CENTER_CD"));
				vo.setEquipId(rs.getString("EQUIP_ID"));
				vo.setEquipNm(rs.getString("EQUIP_NM"));
				vo.setStatus(rs.getString("STATUS"));
				vo.setErrMsg(rs.getString("ERR_MSG"));

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

	private void insertTrg(ArrayList<EquipmentStatusVo> list) {
		Statement stmt = null;

		try {

			stmt = trgCon.createStatement();

			for (EquipmentStatusVo vo : list) {
				/*
				 * BDATE CENTER_CD EQUIP_ID CENTER_NM EQUIP_NM STATUS ERR_MSG REG_DT UPD_DT
				 */
				String sql = "MERGE INTO TB_EQP_STAT                                                                                            " + System.lineSeparator() +
						"USING(                                                                                                            " + System.lineSeparator() +
						"    SELECT '" + vo.getBdate() + "' BDATE, '" + vo.getCenterCd() + "' CENTER_CD, '" + vo.getEquipId() + "' EQP_ID, " + System.lineSeparator() +
						"           '" + vo.getEquipNm() + "' EQUIP_NM, '" + vo.getStatus() + "' STATUS, '" + vo.getErrMsg() + "' ERR_MSG	" + System.lineSeparator() +
						"      FROM DUAL                            																		" + System.lineSeparator() +
						") STAT                                                                                                            " + System.lineSeparator() +
						"ON (TB_EQP_STAT.BDATE = STAT.BDATE AND TB_EQP_STAT.CENTER_CD = STAT.CENTER_CD                                     " + System.lineSeparator() +
						"    AND TB_EQP_STAT.EQUIP_ID = STAT.EQP_ID)											                            " + System.lineSeparator() +
						"WHEN MATCHED THEN                                                                                                 " + System.lineSeparator() +
						"     UPDATE SET STATUS = STAT.STATUS,                                                                             " + System.lineSeparator() +
						"                ERR_MSG = STAT.ERR_MSG,                                                                           " + System.lineSeparator() +
						"                UPD_DT = SYSDATE                                                                                  " + System.lineSeparator() +
						"WHEN NOT MATCHED THEN                                                                                             " + System.lineSeparator() +
						"     INSERT(BDATE, CENTER_CD, EQUIP_ID, EQUIP_NM, STATUS, ERR_MSG, REG_DT, UPD_DT)                                " + System.lineSeparator() +
						"     VALUES(STAT.BDATE, STAT.CENTER_CD, STAT.EQP_ID, STAT.EQUIP_NM, STAT.STATUS, STAT.ERR_MSG, SYSDATE, SYSDATE)";

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