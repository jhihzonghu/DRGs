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
		Job job = Job.getInstance(conf, "xxx");
		job.setJarByClass(driver.class);
		job.setMapperClass(ParserPatientsDataMapper.class);
		job.setReducerClass(ParserPatientsDataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Patients.class);
	//	job.setCombinerClass(reducer.class);
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(
				"hdfs://master:9000/DRGs_InputData/Yearfile.csv"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://master:9000/DRGs_Demo/YearFile_CoVersion"));

		job.waitForCompletion(true);
		long ProcessTime = System.currentTimeMillis() - StartTime;
		AverageTime += ProcessTime;
		System.out.print(AverageTime);

	}

}


		
	
