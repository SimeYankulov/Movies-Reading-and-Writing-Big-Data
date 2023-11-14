package bd.Movies;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MoviesMapper extends MapReduceBase 
	implements Mapper<LongWritable,Text,Text,Text>
{
	String movie;
	String genre;
	String actor;
	int lenght;
	
	@Override
	public void configure(JobConf job) {
		movie = job.get("movie","");
		genre = job.get("genre","");
		actor = job.get("actor","");
		lenght = job.getInt("lenght",0);
	}
	
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		
		String[] columns = value.toString().split(";");
		
		if(movie.isEmpty() || columns[2].toLowerCase().contains(movie)) {
			
			if(genre.isEmpty() || columns[3].toLowerCase().contains(genre))
			{
				
				if(actor.isEmpty() || columns[4].toLowerCase().contains(actor)
									|| columns[5].toLowerCase().contains(actor) )
				{
					
					if(isNumeric(columns[1])) {
					if(lenght == 0 ||   Integer.parseInt(columns[1]) <= lenght ) {

						try {
							String currentMovie = columns[2];
							String currentGenre = columns[3];
							String currentActor1 = columns[4];
							String currentActor2 = columns[5];
							String currentLenght = columns[1];
							String director = columns[6];
					
							Text outputKey = new Text(currentMovie + " " + currentGenre + " " +
							currentActor1 + " " + currentActor2+ " "+director+" ");
							
							output.collect(outputKey, new Text(currentLenght));
	 
						}catch(NumberFormatException ex) {
							System.err.println(value.toString());
						}
					}
					}
				}
		    	}
			}
		
		}
	public static boolean isNumeric(String num) {
		try {
	        int n = Integer.parseInt(num);
	        return true;
	    } catch (NumberFormatException nfe) {
	        return false;
	    }
	}
}	


