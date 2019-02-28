package com.pluspro.ctrlwcs.beans;

public class ChuteResultVo {
	String bdate;
	String centerCd;
	String equipId;
	String chuteNo;
	int progressRt;
	int planQty;
	int qty;
	
	public String getBdate() {
		return bdate;
	}
	public void setBdate(String bdate) {
		this.bdate = bdate;
	}
	public String getCenterCd() {
		return centerCd;
	}
	public void setCenterCd(String centerCd) {
		this.centerCd = centerCd;
	}
	public String getEquipId() {
		return equipId;
	}
	public void setEquipId(String equipId) {
		this.equipId = equipId;
	}
	public String getChuteNo() {
		return chuteNo;
	}
	public void setChuteNo(String chuteNo) {
		
		try {
			chuteNo = String.valueOf(Integer.parseInt(chuteNo));
		}catch(Exception e) {}
		
		this.chuteNo = chuteNo;
	}
	public int getProgressRt() {
		return progressRt;
	}
	public void setProgressRt(int progressRt) {
		this.progressRt = progressRt;
	}
	public int getPlanQty() {
		return planQty;
	}
	public void setPlanQty(int planQty) {
		this.planQty = planQty;
	}
	public int getQty() {
		return qty;
	}
	public void setQty(int qty) {
		this.qty = qty;
	}
	
	
}
