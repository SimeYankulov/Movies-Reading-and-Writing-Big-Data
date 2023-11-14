package bd.Movies;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MovieCatMapper extends MapReduceBase 
	implements Mapper<LongWritable,Text,Text,DoubleWritable>{

	String category;
	
	@Override
	public void configure(JobConf job) {
		category = job.get("category","");
	
	}
	
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
	
			String[] columns = value.toString().split(";");
			
		if(category.isEmpty() || 
				columns[3].toLowerCase().contains(category)) {
						
			try
			{
				String currentCategory = columns[3];
				
				int lenght = Integer.parseInt(columns[1]);
				
				Text outputKey = new Text(currentCategory + " ");
				
				output.collect(outputKey, new DoubleWritable(lenght));
					
			}
			catch(NumberFormatException ex) {
				System.err.println(value.toString());
			}
	}
	}}
