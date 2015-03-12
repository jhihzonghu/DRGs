package DRGs_creativenewtable;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParserPatientsDataReducer extends
		Reducer<Text, Patients, Text, Patients> {
	private int HealthCareArray[] = new int[33];
	private int SelfPayArray[] = new int[33];
	private int IsSpecialOP = 0,TotalHealthyCare=0,TotalSelfPay=0,IsCancer=0;
	private double TotalFee=0;
	private String SpecialOPC = "",OPMaterial="";
	private String BeforeOPDay, AfterOPDay, TotalHostipalDay ,IsLungCancer,IsUsingMIS;
	private String SelfPay, HealthyCare, SelfPayClass;
	private String[] SelfPayStringArray = new String[33];
	private String[] HealthyCareStringArray = new String[33];
	Patients patients = new Patients();

	@Override
	protected void reduce(Text id, Iterable<Patients> Output, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for (Patients Co : Output) {
			
			IsSpecialOP += Integer.valueOf(Co.getIsSpecialOP());
			SpecialOPC += Co.getSpecialOPC();
			OPMaterial += Co.getOPMaterial();
			if(!Co.getIsCancer().equals("")){
				   IsCancer += Integer.valueOf(Co.getIsCancer().trim());
			};
			BeforeOPDay += Co.getBeforeOPDay() + "|";
		    AfterOPDay += Co.getAfterOPDay() + "|";
			TotalHostipalDay += Co.getTotalHostipalDay() + "|";
			SelfPayStringArray = Co.getSelfPay().toString().split(",");
            CoSelfpay();
			 HealthyCareStringArray = Co.getHealthyCare().toString().split(",");
			 CoHealthyCare();
			
		}	
		    SureIsCancer(IsCancer);
		    UsingMIS(IsSpecialOP);
		    OutputSelfandHealthyCarePay();
			PreparePatients();
			context.write(id,patients);
			InitData();
		
	}

	private void UsingMIS(int isSpecialOP2) {
		// TODO Auto-generated method stub
		if(isSpecialOP2>0){
			IsUsingMIS = "Y";
		}else{
			IsUsingMIS="N";
		}
	}

	private void SureIsCancer(int isCancer2) {
		// TODO Auto-generated method stub
		if(isCancer2>0)
		{
			IsLungCancer="Y";
		}else{
			IsLungCancer="N";
		}
	}

	private void CoHealthyCare() {
		// TODO Auto-generated method stub
		 for (int i = 0; i < HealthCareArray.length; i += 1) 
		 {
				if(HealthyCareStringArray[i].equals(""))
				{
					HealthyCareStringArray[i] = "0" ;
				}
				HealthCareArray[i] += Integer.valueOf(HealthyCareStringArray[i].trim());
				
			}
	}

	private void CoSelfpay() {
		// TODO Auto-generated method stub
		 for(int i=0; i<SelfPayStringArray.length;i+=1)
		 {
			 if(SelfPayStringArray[i].equals(""))
			 {
					SelfPayStringArray[i] = "0" ;
				}
			 
			 SelfPayArray[i]+=Integer.valueOf(SelfPayStringArray[i].trim()) ;
			 
		 }
	}

	private void PreparePatients() {
		// TODO Auto-generated method stub
		patients.setBeforeOPDay(BeforeOPDay);
		patients.setAfterOPDay(AfterOPDay);
		patients.setTotalHostipalDay(TotalHostipalDay);
		patients.setIsCancer(IsLungCancer);
		patients.setIsSpecialOP(IsUsingMIS);
		patients.setSpecialOPC(SpecialOPC);
		patients.setOPMaterial(OPMaterial);
		patients.setSelfPay(SelfPay);
		patients.setTotalSelfPay(""+TotalSelfPay);
		patients.setHealthyCare(HealthyCare);
		patients.setTotalHealthyCare(""+TotalHealthyCare);
		patients.setSelfPayClass(SelfPayClass);
	}

	private void OutputSelfandHealthyCarePay() {
		// TODO Auto-generated method stub
		for (int i = 0; i < SelfPayArray.length; i += 1)
		{
			SelfPay += ""+SelfPayArray[i] + ",";
			TotalSelfPay+= (SelfPayArray[i]);
		}
		for (int i =0; i < HealthCareArray.length; i += 1)
		{
			HealthyCare += ""+HealthCareArray[i] + ",";
			TotalHealthyCare+= (HealthCareArray[i]);
		}
		ClassfialSelfpayLevel();
		System.out.println(SelfPay);
		System.out.println(HealthyCare);
	}

	private void ClassfialSelfpayLevel() {
		// TODO Auto-generated method stub
		TotalFee = TotalSelfPay + TotalHealthyCare ; 
		double ClassLevel  ; 
		ClassLevel = (TotalSelfPay*1.0)/TotalFee ; 
		if(ClassLevel>=0 && ClassLevel<=0.1){
			SelfPayClass = "A" ;
		}else{
			if(ClassLevel >0.1 && ClassLevel<=0.2)
			{
				SelfPayClass = "B" ;
			}else{
				SelfPayClass = "C" ;
			}
		}
	}


	private void InitData() {
		// TODO Auto-generated method stub
		System.out.println("Init Data Step");
		IsSpecialOP = 0;
		SpecialOPC = "";
		IsCancer = 0;
		BeforeOPDay = "";
		AfterOPDay = "";
		TotalHostipalDay = "";
		SelfPay="";
		HealthyCare = "";
		SelfPayClass="";
		OPMaterial="";
		this.TotalHealthyCare=0;
		this.TotalSelfPay=0;
		for (int i = 0; i <SelfPayArray.length; i += 1) {
			SelfPayArray[i] =0;
		}
		for (int i = 0; i < HealthCareArray.length; i += 1) {
			HealthCareArray[i] =0;
		}
	}

}
