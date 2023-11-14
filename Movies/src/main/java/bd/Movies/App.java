package bd.Movies;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.net.URI;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;


public class App extends JFrame
{
	private void createForm() {
	    	
	    	JPanel panel = new JPanel(); 
	    
	    	JButton button = new JButton("Search");
	    	button.setBounds(25,300,250,25);
	    	
	    	panel.setLayout(null);
	    	
	    	JLabel resultTypeL = new JLabel("Result Type");
	    	resultTypeL.setBounds(25,50,75,25);
	    	JLabel nameL = new JLabel("Name");
	    	nameL.setBounds(25,100,75,25);
	    	JLabel genreL = new JLabel("Genre");
	    	genreL.setBounds(25,150,75,25);
	    	JLabel actorL = new JLabel("Actor");
	    	actorL.setBounds(25,200,75,25);
	    	JLabel lenghtL = new JLabel("Max Length");
	    	lenghtL.setBounds(25,250,75,25);
	    	
	    	final String [] resultType = {"Movie list","Average lenght by genre"};
	    	
	    	final JComboBox <String> resultTypeC = new JComboBox<String>(resultType);
	    	
	    	resultTypeC.setBounds(125,50,150,25);
	    	
	    	final JTextField nameTF = new JTextField();
	    	final JTextField genreTF = new JTextField();
	    	final JTextField actorTF = new JTextField();
	    	final JTextField lenghtTF = new JTextField();
	    	
	    	
	    	
	    	nameTF.setBounds(125,100,150,25);
	    	genreTF.setBounds(125,150,150,25);
	    	actorTF.setBounds(125,200,150,25);
	    	lenghtTF.setBounds(125,250,150,25);
	    	
	    	panel.add(resultTypeC);
	    	
	    	panel.add(resultTypeL);
	    	panel.add(nameL);
	    	panel.add(genreL);
	    	panel.add(actorL);
	    	panel.add(lenghtL);
	    	
	      	panel.add(nameTF);
	    	panel.add(genreTF);
	    	panel.add(actorTF);
	    	panel.add(lenghtTF);
	    	
	    	panel.add(button);
	    	
	    	add(panel);
	    	setVisible(true);
	    	setSize(350,500);
	    	
	    	button.addActionListener(new ActionListener(){

				public void actionPerformed(ActionEvent e) {
					// TODO Auto-generated method stub
					String item = resultTypeC.getSelectedItem().toString();
					
					//for movie list
					if(item.equals(resultType[0])) {
						
						startHadoopMovieList
							(
									nameTF.getText(),
									genreTF.getText(),
									actorTF.getText(),
									lenghtTF.getText()
								);
						
					}
					else // for length by category
					{
						startHadoopCategory(genreTF.getText());
					}
				}
	    		
	    	});
	    	
	    	resultTypeC.addActionListener (new ActionListener() {
				public void actionPerformed(ActionEvent arg0) {
					// TODO Auto-generated method stub
					String item = resultTypeC.getSelectedItem().toString();
					
					if(item.equals(resultType[1]))
					{
						nameTF.setEditable(false);
						actorTF.setEditable(false);
						lenghtTF.setEditable(false);
					}
					else {
						nameTF.setEditable(true);
						genreTF.setEditable(true);
						actorTF.setEditable(true);
						lenghtTF.setEditable(true);
					}
					
				}
	    	});
	    	
	}

	protected void startHadoopCategory(String category) {
		// TODO Auto-generated method stub
    	
		Configuration conf = new Configuration();
    	
    	JobConf job = new JobConf(conf, App.class);
    	
    	job.set("category", category);
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(DoubleWritable.class);
    	
    	job.setMapperClass(MovieCatMapper.class);
    	job.setReducerClass(MovieCatReducer.class);
    	
    	
    	Path input = new Path("hdfs://127.0.0.1:9000/Movies/film.csv");
		Path output = new Path("hdfs://127.0.0.1:9000/Movies/categoryResult");
		
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		try {
			FileSystem fs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);

			if (fs.exists(output))
				fs.delete(output, true);
			
			RunningJob task = JobClient.runJob(job);
			System.out.println("Is successful : " + task.isSuccessful());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	protected void startHadoopMovieList(String name, String genre, String actor, String lenght) {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		
		JobConf job = new JobConf(conf,App.class);
		
		job.set("movie", name);
		job.set("genre", genre);
		job.set("actor", actor);
		
		if(lenght.isEmpty())
			job.setInt("lenght",0);
		else
			job.setInt("lenght", Integer.parseInt(lenght));
		 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MoviesMapper.class);
		job.setReducerClass(MoviesReducer.class);
		
		Path input = new Path("hdfs://127.0.0.1:9000/Movies/film.csv");
		Path output = new Path("hdfs://127.0.0.1:9000/Movies/moviesResult");

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		try {
			FileSystem fs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);

			if (fs.exists(output))
				fs.delete(output, true);
			
			RunningJob task = JobClient.runJob(job);
			
			System.out.println("Is successful: " + task.isSuccessful());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main( String[] args )
    {
    	App form = new App();
		form.createForm();
    }
}
