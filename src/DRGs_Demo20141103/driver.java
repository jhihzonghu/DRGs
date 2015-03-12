package DRGs_Demo20141103;

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
		FileInputFormat
				.setInputPaths(
						job,
						new Path(
								"hdfs://master:9000/DRGs_Demo/category12_accountTotal/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://master:9000/DRGs_Demo/TestFile"));

		job.waitForCompletion(true);
		long ProcessTime = System.currentTimeMillis() - StartTime;
		AverageTime += ProcessTime;
		System.out.print(AverageTime);

	}

}

class reducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    long Key_index =0;
    
	protected void reduce(LongWritable _key, Text values, Context context)
			throws IOException, InterruptedException {
		String[] output = values.toString().split(";");
		Key_index = Long.valueOf(output[0]);
	  context.write(new LongWritable(Key_index),values);

	}
}

class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
	String Total_OPNum_String = "";
	   String Total_CSCH_String = "";
	   String Key_index ="";
	   String Value_String ="";
	   long Index = 0;
	   int Total_CSPrice_int = 0;
	   int Total_CHPrice_int =0;
	protected void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		String[] Input = ivalue.toString().split(";");
		if(Key_index.equals("")){Key_index = Input[0];}
		if(Key_index.equals(Input[0])){
			Total_OPNum_String += Input[2]+";";
			Total_CSCH_String += Input[3]+";"+Input[4]+";";
			Total_CSPrice_int +=  Total_CSPrice_int+Integer.valueOf(Input[3]);
			Total_CHPrice_int +=Total_CHPrice_int + Integer.valueOf(Input[4]);
		}else{
			Index = Long.valueOf(Key_index.trim());
			Value_String =  ";"+Total_OPNum_String+Total_CSCH_String
		+Total_CSPrice_int+";"+Total_CHPrice_int+";";
			context.write(new LongWritable(Index), new Text(Value_String));
		
			Total_OPNum_String = "";
			  Total_CSCH_String = "";
			   Value_String ="";
			   Index = 0;
			   Total_CSPrice_int = 0;
			   Total_CHPrice_int =0;
			   Key_index = Input[0];
				Total_OPNum_String += Input[2]+";";
				Total_CSCH_String += Input[3]+";"+Input[4]+";";
				Total_CSPrice_int +=  Total_CSPrice_int+Integer.valueOf(Input[3]);
				Total_CHPrice_int +=Total_CHPrice_int + Integer.valueOf(Input[4]);
		}
	}

}
