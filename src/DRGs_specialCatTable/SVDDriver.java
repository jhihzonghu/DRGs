package DRGs_specialCatTable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class SVDDriver {

	public static void main(String[] args) throws IOException,
    ClassNotFoundException, InterruptedException, Exception {
		// TODO Auto-generated method stub
		long StartTime = System.currentTimeMillis();
		long AverageTime = 0;
		Configuration conf=new Configuration();                                                                                                                                                            
    	Job job = Job.getInstance(conf);
        job.setJarByClass(SVDDriver.class);                                                                                                                                                                         
        FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/DRGs_InputData/Yearfile.csv"));//EveryCatAccountTable.csv EveryCatAccountTable.csv
        //FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/DRGs/EveryCatAccountTable.csv"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/FinalTable/SVD"));
        job.setMapperClass(CoSVD.class);
        job.setReducerClass(CoSVDReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);
       
        long ProcessTime = System.currentTimeMillis() - StartTime;
		AverageTime += ProcessTime;
		System.out.print(AverageTime);
	}

}
