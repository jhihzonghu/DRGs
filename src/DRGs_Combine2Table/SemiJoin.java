package DRGs_Combine2Table;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;



	 //private static final Logger logger = LoggerFactory.getLogger(SemiJoin.class);
 class SemiJoinMapper extends Mapper<Object, Text, Text, Combine2Writable> {
     
    	private Combine2Writable combineValues = new Combine2Writable();
        private HashSet<String> joinKeySet = new HashSet<String>();
        private Text flag = new Text();
        private Text joinKey = new Text();
        private Text secondPart = new Text();
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            
        	
            String pathName = ((FileSplit) context.getInputSplit()).getPath().getName();
             System.out.println(pathName);
            if(pathName.endsWith("Cat12.csv")){
                  String[] valueItems = value.toString().split(",");
                   String secondPart_Str =""; 
                    flag.set("0");
                    joinKey.set(valueItems[0]);
                    int maxint = valueItems.length;
                    for(int i=1;i<maxint;i+=1){
                  	  secondPart_Str+= valueItems[i]+"\t";
                  	
                  }
                    secondPart.set(secondPart_Str);
                    combineValues.setFlag(flag);
                    combineValues.setJoinKey(joinKey);
                    combineValues.setSecondPart(secondPart);
                    context.write(combineValues.getJoinKey(), combineValues);
              }
            else if(pathName.endsWith("EveryCatAccountTable.csv")){
                   String[] valueItems = value.toString().split(",");
                   String secondPart_Str =""; 
                   int maxint = valueItems.length;
                    flag.set("1");
                    joinKey.set(valueItems[0]);
                    for(int i=1;i<maxint;i+=1){
                    	  secondPart_Str+=valueItems[i]+"\t";
                    	
                    }
                    secondPart.set(secondPart_Str);
                    combineValues.setFlag(flag);
                    combineValues.setJoinKey(joinKey);
                    combineValues.setSecondPart(secondPart);
                    context.write(combineValues.getJoinKey(), combineValues);
           
            }
         
        }
    }
    
    class SemiJoinReducer extends Reducer<Text, Combine2Writable, Text, Text> {
  
        private ArrayList<Text> leftTable = new ArrayList<Text>();
        private ArrayList<Text> rightTable = new ArrayList<Text>();
        private Text secondPar = null;
        private Text output = new Text();
       // private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SemiJoin.class);
        protected void reduce(Text key, Iterable<Combine2Writable> value, Context context)
                throws IOException, InterruptedException {
    
            leftTable.clear();
            rightTable.clear();

            for(Combine2Writable cv : value){
                secondPar = new Text(cv.getSecondPart().toString());

                if("0".equals(cv.getFlag().toString().trim()))
                {
                    leftTable.add(secondPar);
                }
      
                else if("1".equals(cv.getFlag().toString().trim()))
                {
                    rightTable.add(secondPar);
                }
            }

           System.out.println("Cat12:"+leftTable.toString());
           // System.out.println("EveryCount:"+rightTable.toString());
            
            for(Text leftPart : leftTable){
                for(Text rightPart : rightTable){
                	//System.out.println(key);
                    output.set(leftPart+ "\t" + rightPart);
                    context.write(key, output);
                }
            }
        }
    }
  
    public class SemiJoin {   
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException, Exception {
    	long StartTime = System.currentTimeMillis();
		long AverageTime = 0;
		Configuration conf=new Configuration();                                                                                                                                                            
    	Job job = Job.getInstance(conf);
        job.setJarByClass(SemiJoin.class);                                                                                                                                                                         
        FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/DRGs/Cat12.csv"));//EveryCatAccountTable.csv EveryCatAccountTable.csv
        FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/DRGs/EveryCatAccountTable.csv"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/FinalTable/DT"));
        job.setMapperClass(SemiJoinMapper.class);
        job.setReducerClass(SemiJoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Combine2Writable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Combine2Writable.class);
        job.waitForCompletion(true);
       
        long ProcessTime = System.currentTimeMillis() - StartTime;
		AverageTime += ProcessTime;
		System.out.print(AverageTime);
		
		
    }


}