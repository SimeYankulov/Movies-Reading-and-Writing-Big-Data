package bd.Movies;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MovieCatReducer extends MapReduceBase
 implements Reducer<Text,DoubleWritable,Text,DoubleWritable>
{

	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		
		
		int sum = 0;
		int count = 0;
		
		while(values.hasNext()) 
		{
			sum+= values.next().get();
			count++;
		}
		
		output.collect(key, new DoubleWritable(sum/count));
		
	}

}
