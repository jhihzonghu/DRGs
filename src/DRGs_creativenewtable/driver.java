package DRGs_creativenewtable;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class driver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		long StartTime = System.currentTimeMillis();
		long AverageTime = 0;
//		conf.set("mapreduce.jobtracker.address", "local");
//		//conf.set("mapred.child.java.opts", "-Xmx200m");
//		conf.set("mapred.job.tracker", "local");
//		//conf.set("mapreduce.task.io.sort.mb", "100");
//		conf.set("fs.defaultFS","file:///");
//		conf.set("fs.default.name","file:///");
//		conf.set("dfs.replication", "1");
		Job job = new Job(conf, "xxx");
		job.setJarByClass(driver.class);
		job.setMapperClass(ParserPatientsDataMapper.class);
		job.setReducerClass(ParserPatientsDataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Patients.class);
//		job.setCombinerClass(reducer.class);
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(
				"hdfs://hadoop01:9000/healthycarebill/3YearFile.csv"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://hadoop01:9000/DRGs_output"));
//		FileInputFormat.setInputPaths(job, new Path(
//				"/home/jhihzonghu/3YearFile.csv"));
//		FileOutputFormat.setOutputPath(job, new Path(
//				"/home/jhihzonghu/Output"));
		job.waitForCompletion(true);
		long ProcessTime = System.currentTimeMillis() - StartTime;
		AverageTime += ProcessTime;
		System.out.print(AverageTime);

	}

}


		
	
