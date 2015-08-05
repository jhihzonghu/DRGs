package DRGs_creativenewtable;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ParserPatientsDataMapper extends
		Mapper<Object, Text, Text, Patients> {
	private String[] SpiltThePatients;
	int[] AccountHealthyCare = new int[33];
	int[] AccountSelfPay = new int[33];
	int[] OPCodeCounter = new int[8];

	int TOTALCOLUMN = 29, SERIALNUMBER = 0;
	String INDEXKEY = "", TEMPKEY = "", ISCANCER = "", SELFPAYPERCENT,
			Class10_ItemName = "", Class12_ItemName = "", HadOP = "",
			CombineDay = "";
	String SelfPay = "", HealthCarePay = "";
	long Beforeday, Afterday, diff;
	int totalselfpay = 0, totalhealthycare = 0, total = 0, Count = 1,
			RowCounter = 0, cat10Count = 0;
	Patients patients = new Patients();

	@Override
	protected void map(Object key, Text inputLine, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		RowCounter += 1;
		SpiltThePatients = inputLine.toString().split(",");
		if (IsCorrectRecord(SpiltThePatients)) {
			TEMPKEY = SpiltThePatients[8];
			if (isSameINDEXKEY()) {
				KeepWriteData();
				ChangeINDEXKey();
			} else {
				WriteData();
				PreparePatientsData();
				context.write(new Text(INDEXKEY + "|" + CombineDay), patients);
				InitData();
				// KeepWriteData();
				ChangeINDEXKey();
				KeepWriteData();
			}
		}
	}

	private void PreparePatientsData() {
		// TODO Auto-generated method stub
		patients.setBeforeOPDay("" + Beforeday);
		patients.setAfterOPDay("" + Afterday);
		patients.setTotalHostipalDay("" + diff);
		patients.setIsCancer(ISCANCER);
		patients.setIsSpecialOP(HadOP);
		patients.setSpecialOPC(Class12_ItemName);
		patients.setOPMaterial(Class10_ItemName);
		patients.setSelfPay(SelfPay);
		patients.setHealthyCare(HealthCarePay);
		patients.setTotalSelfPay("" + totalselfpay);
		patients.setTotalHealthyCare("" + totalhealthycare);
		patients.setSelfPayClass(SELFPAYPERCENT);
	}

	private void KeepWriteData() {
		// TODO Auto-generated method stub
		IsCancer();
		AccountingCatTotal();
	}

	private void AccountingCatTotal() {
		// TODO Auto-generated method stub
		int CatIndex;
		if (isNumeric(SpiltThePatients[11].trim())&&(!SpiltThePatients[11].equals(""))) {
			// System.out.println(inputLine[11]);
			CatIndex = Integer.valueOf(SpiltThePatients[11].trim());
			if (isNumeric(SpiltThePatients[11].trim())
					&& CatIndex < AccountHealthyCare.length) {
				if (CatIndex == 12) {
					// Class12_ItemName += SpiltThePatients[12]+"|" ;
					encodingClass12(SpiltThePatients[12]);
					diff = AccountTotalDay(SpiltThePatients[9],
							SpiltThePatients[10]);
					Beforeday = AccountBeforeOPDays(SpiltThePatients[14],
							SpiltThePatients[9]);
					Afterday = AccountAfterDay(SpiltThePatients[10],
							SpiltThePatients[14]);
				}
				if (CatIndex == 10 && cat10Count <= 100) {
					Class10_ItemName += this.SpiltThePatients[12] + "|";
					cat10Count += 1;
				}
				AccountHealthyCare[CatIndex] += Integer
						.valueOf(SpiltThePatients[17]);
				AccountSelfPay[CatIndex] += Integer
						.valueOf(SpiltThePatients[27]);

			}

		}

	}

	private void encodingClass12(String spiltThePatients2) {
		// TODO Auto-generated method stub
		String[] MIS = { "P067047B", "P067048B", "P067049B", "P067050B",
				"P067051B", "P068049B"
		// "50080057","50080010","50080061","50000806","50080057","50000389","50001618"
		};
		String[] MISEncoding = { "A", "B", "C", "D", "E", "F" };
		for (int i = 0; i < MIS.length; i += 1) {
			if (spiltThePatients2.equals(MIS[i])) {
				Class12_ItemName += MISEncoding[i];
			}
		}
	}

	private long AccountAfterDay(String day1, String day2) {
		// TODO Auto-generated method stub
		SimpleDateFormat date = new SimpleDateFormat("yyyy/MM/dd");
		// BeforeOPDaysBL = false ;
		try {
			Date inhospital = date.parse(day1);
			Date billday = date.parse(day2);
			Afterday = (inhospital.getTime() - billday.getTime());
			Afterday = TimeUnit.DAYS.convert(Afterday, TimeUnit.MILLISECONDS);
		} catch (ParseException e) {
			// TODO Auto-generated catch block e.printStackTrace();
		}
		return Afterday;
	}

	private boolean isNumeric(String inputLine2) {
		// TODO Auto-generated method stub
		Pattern pattern = Pattern.compile("[0-9]*");
		Matcher isNum = pattern.matcher(inputLine2);
		if (!isNum.matches()) {
			return false;
		}
		return true;
	}

	private void IsCancer() {
		// TODO Auto-generated method stub
		String[] CancerCode = { "162.0", "162.1", "162.2", "162.3", "162.4",
				"162.5", "162.6", "162.8", "162.9" };
		int[] isCancerCode = new int[CancerCode.length];
		int isCancerint = 0;
		for (int j = 0; j < 8; j += 1) // check is cancer code
		{

			for (int i = 0; i < CancerCode.length; i += 1) {
				if (SpiltThePatients[j].equals(CancerCode[i])) {
					isCancerCode[i] += 1;
				}
			}
		}
		for (int i = 0; i < isCancerCode.length; i += 1) {
			isCancerint += isCancerCode[i];
			if (isCancerint > 0) {
				ISCANCER = "1";
			} else {
				ISCANCER = "0";
			}
			isCancerCode[i] = 0;
		}
	}

	private void ChangeINDEXKey() {
		// TODO Auto-generated method stub
		String InDate, OutDate;
		INDEXKEY = this.SpiltThePatients[8];
		InDate = SpiltThePatients[9].replace("/", "");
		OutDate = SpiltThePatients[10].replace("/", "");
		CombineDay = InDate + OutDate;
	}

	private void InitData() {
		// TODO Auto-generated method stub
		totalselfpay = 0;
		total = 0;
		totalhealthycare = 0;
		Afterday = 0;
		ISCANCER = "";
		Class12_ItemName = "";
		HadOP = "";
		CombineDay = "";
		HealthCarePay = "";
		SelfPay = "";
		Class10_ItemName = "";
		// SearialNumber = "";
		for (int i = 0; i < AccountHealthyCare.length; i += 1) {
			AccountHealthyCare[i] = 0;
			AccountSelfPay[i] = 0;
		}
	}

	private long AccountBeforeOPDays(String billdaystr, String inday) {
		// TODO Auto-generated method stub
		SimpleDateFormat date = new SimpleDateFormat("yyyy/MM/dd");
		// BeforeOPDaysBL = false ;
		try {
			Date inhospital = date.parse(inday);
			Date billday = date.parse(billdaystr);
			Beforeday = (billday.getTime() - inhospital.getTime());
			Beforeday = TimeUnit.DAYS.convert(Beforeday, TimeUnit.MILLISECONDS);
		} catch (ParseException e) {
			// TODO Auto-generated catch block e.printStackTrace();
		}
		return Beforeday;
	}

	private long AccountTotalDay(String hostpitaled, String out) {
		// TODO Auto-generated method stub
		SimpleDateFormat date = new SimpleDateFormat("yyyy/MM/dd");
		try {
			Date inhospital = date.parse(hostpitaled);
			Date outhostpitaled = date.parse(out);
			diff = (outhostpitaled.getTime() - inhospital.getTime());
			diff = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return diff;
	}

	private void CalSelfPay(int total2, int totalselfpay2) {
		// TODO Auto-generated method stub
		double test = (totalselfpay2 * 1.0) / total2;
		if (test >= 0 && test <= 0.1) {
			SELFPAYPERCENT = "A";
		} else {
			if (test > 0.1 && test <= 0.2) {
				SELFPAYPERCENT = "B";
			} else {
				SELFPAYPERCENT = "C";
			}
		}
	}

	private void ConformityOutputValue() {
		// TODO Auto-generated method stub
		for (int i = 1; i < AccountHealthyCare.length; i += 1) {

			totalselfpay += AccountSelfPay[i];
			totalhealthycare += AccountHealthyCare[i];
			SelfPay += "," + AccountSelfPay[i];
			HealthCarePay += "," + AccountHealthyCare[i];
		}
	}

	private void WriteData() {
		// TODO Auto-generated method stub

		ConformityOutputValue();
		if (Class12_ItemName.equals("")) {
			HadOP = "0";
		} else {
			HadOP = "1";
		}
		total += totalselfpay + totalhealthycare;
		CalSelfPay(total, totalselfpay);

		// System.out.println( IsCancerWeight);
		// SearialNumber =inputLine[1].replace("/",
		// "")+inputLine[2].replace("/", "") ;
	}

	private boolean isSameINDEXKEY() {
		// TODO Auto-generated method stub
		if (INDEXKEY.equals(TEMPKEY)) {
			return true;
		} else {
			return false;
		}

	}

	private boolean IsCorrectRecord(String[] spiltThePatients2) {
		// TODO Auto-generated method stub
		if (spiltThePatients2.length == TOTALCOLUMN) {
			return true;
		} else {
			return false;
		}
	}

}
