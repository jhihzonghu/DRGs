package DRGs_specialCatTable;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class driver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		long StartTime = System.currentTimeMillis();
		long AverageTime = 0;
		Job job = Job.getInstance(conf, "xxx");
		job.setJarByClass(driver.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PCN.class);
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat
				.setInputPaths(
						job,
						new Path(
								"hdfs://master:9000/DRGs_InputData/Yearfile.csv"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://master:9000/DRGs_Demo/Cat12Patients"));

		job.waitForCompletion(true);
		long ProcessTime = System.currentTimeMillis() - StartTime;
		AverageTime += ProcessTime;
		System.out.print(AverageTime);

	}

}

class reducer extends Reducer<Text, PCN, Text, PCN> {
    long Key_index =0;
    //Text txt = new Text();
	String[] MIS = {
			"P067047B","P067048B","P067049B","P067050B","P067051B","P068049B"
		//"50080057","50080010","50080061","50000806","50080057","50000389","50001618"
		};
	
	String output,part2,InCode; 
	int[] MISCode = new int[MIS.length];
	String[] MISCode2 = {"A","B","C","D","E","F"};
	int TestCode ;
	Text txt1 = new Text();
	PCN pcn = new PCN();
	protected void reduce(Text _key, Iterable<PCN> values, Context context)
			throws IOException, InterruptedException {
		
		  for(PCN txt: values)
		  {
			  output = ""; 
			  InCode="";
			  TestCode=0;
			  part2 = txt.getPart2(); 
			  ///System.out.println(part2);
			  
			  String[] pcn = txt.getPCN().split(",") ; 
			 // String[] getPN = txt.toString().split(",");
			 for(int i = 0 ; i <pcn.length ; i+=1)
			 {
				String getPNCode = pcn[i] ;
				for(int j=0 ; j < MIS.length ; j+=1)
				{
					if(getPNCode.equals(MIS[j]))
					{
						MISCode[j] += 1 ; 
				
					}
					
				 }	
			  }
			 
			 for(int i=0 ; i<MISCode.length ; i+=1)
			 {
				output += "" + MISCode[i] ; 
				InCode += MISCode2[MISCode[i]];
				TestCode+= MISCode[i] ;
				MISCode[i] = 0 ; 
			 }
		
		  }
		if (TestCode > 0) 
		{
			
			pcn.setPCN(InCode);
			System.out.println(pcn);
			context.write(_key, new PCN(part2, InCode));
		}
			
	}
	
}

class Map extends Mapper<LongWritable, Text, Text, PCN> {
    String PayMentList_Str = "",KeyIndex_Str = "", Part2="",IndexKey="";
    long KeyIndex_Long = 0,count2=0 ,diff, BeforeOPDays,AfterOPDays ; 
    boolean ClassWriteflag = false , KeyValueflag = false , BeforeOPDaysBL=true; 
    NumberFormat Percent ;
    String[] inputLine ;
    int CatIndex,totalcolum=29,SerialNumber=0 ;
    PCN pcn = new PCN();
	protected void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
	             inputLine = ivalue.toString().split(","); 
	             if(inputLine.length==totalcolum)
	             {
	            	  KeyIndex_Str=inputLine[8];
	            	  if(isSameINDEXKEY())
	            	  {  		   
	            		  if(IsClassNumIs12())
	            		  {
	            			  PayMentList_Str += ","+inputLine[12];
	            		  }
	            		  IndexKey = inputLine[8];
	            	  }else{
	            		  SerialNumber +=1;
	            		  // Write KeyValue
	            			AccountBeforeOPDays(inputLine[14], inputLine[9]);
	            			AccountTotalDay(inputLine[9], inputLine[10]);
	            		  AfterOPDays = diff - BeforeOPDays ;
	            		  Part2 = ","+diff+","+BeforeOPDays+","+AfterOPDays ; 
	            		  context.write(new Text(IndexKey+SerialNumber),  new PCN(Part2, PayMentList_Str));
	            		  InitData() ;
	            		  IndexKey=inputLine[8];
	            		  if(IsClassNumIs12())
	            		  {
	            			  PayMentList_Str += ","+inputLine[12];
	            		  }
	            	  }
	             }
		}
	
	
	

	private boolean isSameINDEXKEY() {
		// TODO Auto-generated method stub
		if(IndexKey.equals(KeyIndex_Str)){
			return true;
		}else{
			return false;
		}
	
	}




	private boolean IsClassNumIs12() {
		// TODO Auto-generated method stub
		if (inputLine[11].trim().equals("12") ){
			return true ; 
		}else{
			return false;
		}
	}
	//	{
	//		if (BeforeOPDaysBL)
	//		{
	//			AccountBeforeOPDays(inputLine[14], inputLine[9]);
	//			AccountTotalDay(inputLine[9], inputLine[10]);
	//		}
	//		PayMentList_Str += "," + inputLine[12];
	//		count2 += 1;
	//		ClassWriteflag = true;

			// }
		//}
	




	




	private void PercentCal() {
		// TODO Auto-generated method stub
		Percent  = NumberFormat.getPercentInstance();
		Percent.setMinimumFractionDigits(2);
		//BeforeOPDaysRate = Percent.format((BeforeOPDays*1.0)/diff);
		//AfterOPDaysRate = Percent.format((AfterOPDays*1.0)/diff);
	}



	private void AccountBeforeOPDays(String billdaystr, String inday) {
		// TODO Auto-generated method stub
		SimpleDateFormat date = new SimpleDateFormat("yyyy/MM/dd");
		BeforeOPDaysBL = false ; 
		try {
			Date inhospital = date.parse(inday);
			Date billday = date.parse(billdaystr);
			BeforeOPDays = (billday.getTime() - inhospital.getTime()) ; 
			BeforeOPDays =TimeUnit.DAYS.convert(BeforeOPDays, TimeUnit.MILLISECONDS);
			} catch (ParseException e) {
			// TODO Auto-generated catch block			e.printStackTrace();
		}
	}



	private boolean ChecMinInvasiveSurgery(String MISStr) {
		// TODO Auto-generated method stub
		String[] MIS = {"P067047B","P067048B","P067049B","P067050B","P067051B","P068049B",
		              "P070416B","P070417B","P071223B","P071224B","P071225B","P083085B"};
		for(int i=0 ;i<MIS.length;i+=1){
			if(MISStr.equals(MIS[i]))
			{
				return true;
			}
		}
		return false ; 
	}
	
	private void AccountTotalDay(String hostpitaled, String out) {
		// TODO Auto-generated method stub
		SimpleDateFormat date = new SimpleDateFormat("yyyy/MM/dd");
		 
		try {
			Date inhospital = date.parse(hostpitaled);
			Date outhostpitaled = date.parse(out);
			diff = (outhostpitaled.getTime() - inhospital.getTime()) ; 
			diff =TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
		   // System.out.println ("Days: " + TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS));
		} catch (ParseException e) {
			// TODO Auto-generated catch block			e.printStackTrace();
		}
	}
	private void InitData() {
		// TODO Auto-generated method stub
		  PayMentList_Str ="";
		  //SerialNumber="";
		  count2 = 0 ;
		  BeforeOPDaysBL=true ;
		  ClassWriteflag =false ;
	}
}


