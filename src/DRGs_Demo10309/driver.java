package DRGs_Demo10309;

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
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DRGsWritable.class);
		job.setCombinerClass(reducer.class);
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(
				"hdfs://master:9000/DRGs_Demo/category12_accountTotal/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://master:9000/category12"));
		job.waitForCompletion(true);
		long ProcessTime = System.currentTimeMillis() - StartTime;
		AverageTime += ProcessTime;
		System.out.print(AverageTime);

	}

}

class reducer extends
		Reducer<Text, DRGsWritable, Text, DRGsWritable> {

	protected void reduce(Text _key, Iterable<DRGsWritable> values,
			Context context) throws IOException, InterruptedException {
		long TotalHostpitaledFee = 0;
		long HealthyCareFee = 0;
		long TotalSelfPayFee = 0;
		long Count = 0;
		for (DRGsWritable longValue : values) {
			TotalHostpitaledFee += longValue.getTotalHostpitaledFee();
			HealthyCareFee += longValue.getHealthyCareFee();
			TotalSelfPayFee += longValue.getTotalSelfPayFee();
			Count += longValue.getCount();
		}
		context.write(_key, new DRGsWritable(TotalHostpitaledFee, HealthyCareFee, TotalSelfPayFee,Count));
		
		
	}
}


class Map extends Mapper<LongWritable, Text, Text, DRGsWritable> {
	

	protected void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
	String[] val = ivalue.toString().split(";");
	String AccountName = val[2]; 
	long TotalHostpitaledFee =Long.valueOf(val[3]);
	long HealthyCareFee = Long.valueOf(val[4]);
	long TotalSelfPayFee = Long.valueOf(val[5]);
	long Count = 1 ; 
	context.write(new Text(AccountName), new DRGsWritable(TotalHostpitaledFee, HealthyCareFee, TotalSelfPayFee,Count));

	}

}
