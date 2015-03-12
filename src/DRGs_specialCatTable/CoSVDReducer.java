package DRGs_specialCatTable;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CoSVDReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    int Sum = 0 ; 
	@Override
	protected void reduce(Text ID, Iterable<IntWritable> counter,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		
		for(IntWritable counter1 : counter){
			Sum += counter1.get();
		}
		   context.write(ID, new IntWritable(Sum));
		   Sum = 0;
	}

}
