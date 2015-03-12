package DRGs_Demo10310;

import java.io.IOException;

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
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setCombinerClass(reducer.class);
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(
				"hdfs://master:9000/DRGs/10201_6.csv"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://master:9000/DRGs_Demo/category12_accountTotal"));

		job.waitForCompletion(true);
		long ProcessTime = System.currentTimeMillis() - StartTime;
		AverageTime += ProcessTime;
		System.out.print(AverageTime);

	}

}

class reducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	protected void reduce(LongWritable _key, Text values, Context context)
			throws IOException, InterruptedException {
		// DataWritable val = new DataWritable();
		// String ItemName="";
		String[] val = values.toString().split(";");
		String AccountNum = "";
		String Register = "";
		int TotalHostpitaledFee = 0, HealthyCareFee = 0, TotalSelfPayFee = 0;
		if (Register.equals("")) {
			Register = val[2];
			AccountNum = Register;
		}
		if (AccountNum.equals(val[2])) {
			TotalHostpitaledFee += Integer.valueOf(val[3]);
			HealthyCareFee += Integer.valueOf(val[4]);
			TotalSelfPayFee += Integer.valueOf(val[5]);
			String output = ";" + AccountNum + ";" + TotalHostpitaledFee + ";"
					+ HealthyCareFee + ";" + TotalSelfPayFee;
			context.write(_key, new Text(output));

		} else {
			TotalHostpitaledFee = 0;
			HealthyCareFee = 0;
			TotalSelfPayFee = 0;
			AccountNum = val[2];
			TotalHostpitaledFee += Integer.valueOf(val[3]);
			HealthyCareFee += Integer.valueOf(val[4]);
			TotalSelfPayFee += Integer.valueOf(val[5]);
			String output = ";" + AccountNum + ";" + TotalHostpitaledFee + ";"
					+ HealthyCareFee + ";" + TotalSelfPayFee;
			context.write(_key, new Text(output));
		}

	}

}

class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
	public String Register, Combine2Colum = "", ItemName_Global = "";
	int ItemArrayIndex = 0;
	long index = 1, RegisterRate;
	int ItemTotal = 0, HealthyFee, PartionPayment, SelfPay;
	Long TotalPrice;

	protected void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		String category, AccountNum, ItemName, OP_Date, TotalHostpitaledFee, TotalSelfPayFee, HealthyCareFee; // Pre_OP_stay,
																												// Post_OP_stay,;
		String[] InputData = ivalue.toString().split(";");
		// check the row to InputData array
		// have enough colums
	
			if (Combine2Colum.equals("")) { // initial data
				Combine2Colum = InputData[1] + ";" + InputData[2];
				System.out.println(Combine2Colum);
				Register = Combine2Colum;
				category = InputData[3];
				System.out.println(Register);
			}

			category = InputData[3];
			AccountNum = InputData[4];
			ItemName = InputData[5];
			OP_Date = InputData[6];
			TotalHostpitaledFee = InputData[7];
			HealthyCareFee = InputData[9];
			TotalSelfPayFee = InputData[19];

			if (Register.equals(InputData[1] + ";" + InputData[2])) {// check
																		// the
				if (category.equals("12")) {
					// /HealthyFee = Integer.valueOf(HealthyCareFee);
					// TotalPrice = Long.valueOf(TotalHostpitaledFee);
					// TotalPrice += Integer.valueOf(TotalHostpitaledFee);
					String OutputStr = ";" + category + ";" + AccountNum + ";"
							+ TotalHostpitaledFee + ";" + HealthyCareFee + ";"
							+ TotalSelfPayFee + ";";
					context.write(new LongWritable(index), new Text(OutputStr));

				}
				// String OutputStr =
				// ";"+Register+";"+category+";"+AccountNum+";"
				// +ItemName +";" +OP_Date +";" +TotalHostpitaledFee +";"
				// +HealthyCareFee +";" +TotalSelfPayFee +";" ;
				// context.write(new LongWritable(index), new Text(OutputStr));

			} else {
				index += 1;
				Register = InputData[1] + ";" + InputData[2];
				if (category.equals("12")) {
					// HealthyFee = Integer.valueOf(HealthyCareFee);
					// TotalPrice = Long.valueOf(TotalHostpitaledFee);
					// TotalPrice += Integer.valueOf(TotalHostpitaledFee);
					String OutputStr = ";" + category + ";" + AccountNum + ";"
							+ TotalHostpitaledFee + ";" + HealthyCareFee + ";"
							+ TotalSelfPayFee + ";";
					context.write(new LongWritable(index), new Text(OutputStr));

					// HealthyFee = 0;
					// TotalPrice = 0 ;
				}

			}
			// context.write(new LongWritable(index), new Text());

		}

	}
