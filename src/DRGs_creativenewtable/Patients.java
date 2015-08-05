package DRGs_creativenewtable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Patients implements Writable {
    private String BeforeOPDay, AfterOPDay,TotalHostipalDay,IsCancer,IsSpecialOP,SpecialOPC,OPMaterial;
    private String SelfPay,HealthyCare,SelfPayClass, TotalSelfPay, TotalHealthyCare ; 
    private String Value ;
	
	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		BeforeOPDay = input.readUTF();
		AfterOPDay = input.readUTF();
		TotalHostipalDay = input.readUTF();
		IsCancer = input.readUTF();
		IsSpecialOP =input.readUTF();
		SpecialOPC = input.readUTF() ; 
		OPMaterial = input.readUTF();
		SelfPay = input.readUTF() ; 
		HealthyCare = input.readUTF();
		TotalSelfPay = input.readUTF();
		TotalHealthyCare = input.readUTF();
		SelfPayClass = input.readUTF();
	}


	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		output.writeUTF(BeforeOPDay);
		output.writeUTF(AfterOPDay);
		output.writeUTF(TotalHostipalDay);
		output.writeUTF(IsCancer);
		output.writeUTF(IsSpecialOP);
		output.writeUTF(SpecialOPC);
		output.writeUTF(OPMaterial);
		output.writeUTF(SelfPay);
		output.writeUTF(HealthyCare);
		output.writeUTF(TotalSelfPay);
		output.writeUTF(TotalHealthyCare);
		output.writeUTF(SelfPayClass);
	
	
		
	}

	

	/**
	 * @param beforeOPDay
	 * @param afterOPDay
	 * @param totalHostipalDay
	 * @param isCancer
	 * @param isSpecialOP
	 * @param specialOPC
	 * @param selfPay
	 * @param healthyCare
	 * @param selfPayClass
	 */
	public Patients(){};
	public Patients(String IsSpecialOP, String SpecialOPC,String SelfPay,String HealthyCare)
	{
		this.IsSpecialOP = IsSpecialOP;
		this.SpecialOPC =SpecialOPC ; 
		this.SelfPay = SelfPay ; 
		this.HealthyCare = HealthyCare ; 
	};
	public Patients(String beforeOPDay, String afterOPDay,
			String totalHostipalDay, String isCancer, String isSpecialOP,
			String specialOPC,String oPMaterial, String selfPay, String healthyCare,String totalSelfPay,String totalHealthyCare,
			String selfPayClass) {
		//super();
		BeforeOPDay = beforeOPDay;
		AfterOPDay = afterOPDay;
		TotalHostipalDay = totalHostipalDay;
		IsCancer = isCancer;
		IsSpecialOP = isSpecialOP;
		SpecialOPC = specialOPC;
		OPMaterial = oPMaterial ;
		SelfPay = selfPay;
		HealthyCare = healthyCare;
		TotalSelfPay = totalSelfPay;
		TotalHealthyCare = totalHealthyCare;
		SelfPayClass = selfPayClass;
	}

	@Override
	public String toString() {
		return BeforeOPDay + "\t"+ AfterOPDay + "\t" + TotalHostipalDay+ "\t" + IsCancer + "\t" + IsSpecialOP
				+ "\t" + SpecialOPC +"\t"+this.OPMaterial+"\t" + SelfPay+ "\t" + HealthyCare + "\t"+TotalSelfPay+"\t"+TotalHealthyCare+"\t"+ SelfPayClass ;
		
	}

	public String getBeforeOPDay() {
		return BeforeOPDay;
	}

	public void setBeforeOPDay(String beforeOPDay) {
		BeforeOPDay = beforeOPDay;
	}

	public String getAfterOPDay() {
		return AfterOPDay;
	}

	public void setAfterOPDay(String afterOPDay) {
		AfterOPDay = afterOPDay;
	}

	public String getTotalHostipalDay() {
		return TotalHostipalDay;
	}

	public void setTotalHostipalDay(String totalHostipalDay) {
		TotalHostipalDay = totalHostipalDay;
	}

	public String getIsCancer() {
		return IsCancer;
	}

	public void setIsCancer(String isCancer) {
		IsCancer = isCancer;
	}

	public String getIsSpecialOP() {
		return IsSpecialOP;
	}

	public void setIsSpecialOP(String isSpecialOP) {
		IsSpecialOP = isSpecialOP;
	}

	public String getSpecialOPC() {
		return SpecialOPC;
	}

	public void setSpecialOPC(String specialOPC) {
		SpecialOPC = specialOPC;
	}

	public String getSelfPay() {
		return SelfPay;
	}

	public void setSelfPay(String selfPay) {
		SelfPay = selfPay;
	}

	public String getHealthyCare() {
		return HealthyCare;
	}

	public void setHealthyCare(String healthyCare) {
		HealthyCare = healthyCare;
	}

	public String getSelfPayClass() {
		return SelfPayClass;
	}

	public void setSelfPayClass(String selfPayClass) {
		SelfPayClass = selfPayClass;
	}
	public String getOPMaterial() {
		return OPMaterial;
	}

	public void setOPMaterial(String oPMaterial) {
		OPMaterial = oPMaterial;
	}

	public String getTotalSelfPay() {
		return TotalSelfPay;
	}

	public void setTotalSelfPay(String totalSelfPay) {
		TotalSelfPay = totalSelfPay;
	}

	public String getTotalHealthyCare() {
		return TotalHealthyCare;
	}

	public void setTotalHealthyCare(String totalHealthyCare) {
		TotalHealthyCare = totalHealthyCare;
	}
}
