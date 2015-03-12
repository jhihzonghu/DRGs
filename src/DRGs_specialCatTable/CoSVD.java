package DRGs_specialCatTable;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CoSVD extends Mapper<Object, Text, Text, IntWritable> {
	String[] inputSpilt;
	String ID, Item, CLASSNUM="10";
	int TOTALCOLUMNS = 29;

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// super.map(key, value, context);
		inputSpilt = value.toString().split(",");
		if (isCorrectLength()) {
			ID = inputSpilt[8];
            if(inputSpilt[11].equals(CLASSNUM)){
            	context.write(new Text(ID+","+inputSpilt[12]+","), new IntWritable(1));
            }
		}
	}

	

	
	private boolean isCorrectLength() {
		// TODO Auto-generated method stub
		if (inputSpilt.length == TOTALCOLUMNS) {
			return true;
		} else {
			return false;
		}
	}
}
