package colorCount;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ColorCount extends Configured implements Tool {

	
	//Map Class
	   static public class ColorCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	      
		  /*
		   * Create a regular expression pattern to extract all alphabet consisted words
		   * */
		  final static Pattern Alphabet = Pattern.compile("[a-zA-Z]+");
		  final private static LongWritable ONE = new LongWritable(1);
	      private Text ColourNames = new Text();

	      @Override
	      protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
	       
	    	  Matcher matcher = Alphabet.matcher(text.toString());
	            while (matcher.find()) {
	          /*
	           * Change all words into upper case and match the with specific words of 7 colours Pre-defined
	           * */
	            	String tem= matcher.group().toUpperCase();
	           
	          /*
	           * Save the Color Name with number of counts into mapper output by different colors from 'RED' to 'PURPLE'
	           * */
	           if(tem.equals("RED"))
	           {
	        	 String RED="The Totle Appearance of color 'Red' is :"; 
	        	 ColourNames.set(RED);
	        	 context.write(ColourNames, ONE);
	           }
	           else if(tem.equals("ORANGE"))
	           {
	        	 String ORANGE="The Totle Appearance of color 'Orange' is :";
	        	 ColourNames.set(ORANGE);
	        	 context.write(ColourNames, ONE);
	           }
	           else if(tem.equals("YELLOW"))
	           {
	        	 String YELLOW="The Totle Appearance of color 'Yellow' is :";
	        	 ColourNames.set(YELLOW);
	        	 context.write(ColourNames, ONE);
	           }
	           else if(tem.equals("GREEN"))
	           {
	        	 String GREEN="The Totle Appearance of color 'Green' is :";
	        	 ColourNames.set(GREEN);
	        	 context.write(ColourNames, ONE);
	           }
	           else if(tem.equals("CYAN"))
	           {
	        	 String CYAN="The Totle Appearance of color 'Cyan' is :";
	        	 ColourNames.set(CYAN);
	        	 context.write(ColourNames, ONE);
	           }
	           else if(tem.equals("BLUE"))
	           {
	        	 String BLUE="The Totle Appearance of color 'Blue' is :";
	        	 ColourNames.set(BLUE);
	        	 context.write(ColourNames, ONE);
	           }
	           else if(tem.equals("PURPLE"))
	           {
	        	 String PURPLE="The Totle Appearance of color 'Purple' is :";
	        	 ColourNames.set(PURPLE);
	        	 context.write(ColourNames, ONE);
	           }
	               
	         }
	      }   
	   }

	   //Reducer
	   static public class ColorCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	      private LongWritable total = new LongWritable();

	      @Override
	      protected void reduce(Text token, Iterable<LongWritable> counts, Context context)
	            throws IOException, InterruptedException {
	         long n = 0;
	         //Calculate sum of counts
	         for (LongWritable count : counts)
	            n += count.get();
	         total.set(n);

	         context.write(token, total);
	      }
	   }
	   
	   
	 

	   public int run(String[] args) throws Exception {
	      Configuration configuration = getConf();

	      //Initializing Map Reduce Job
	      Job job = new Job(configuration, "Colour Count");
	      
	      //Set Map Reduce main jobconf class
	      job.setJarByClass(ColorCount.class);

	      //Set Mapper class
	      job.setMapperClass(ColorCountMapper.class);
	      
	      /*
	       * Set reducer Number as 1, to put all results into one Reducer
	       * */
	      job.setNumReduceTasks(1);
	      
	      //set Reducer class
	      job.setReducerClass(ColorCountReducer.class);
	      
	      //set Input Format
	      job.setInputFormatClass(SequenceFileInputFormat.class);
	      
	      //set Output Format
	      job.setOutputFormatClass(TextOutputFormat.class);

	      //set Output key class
	      job.setOutputKeyClass(Text.class);
	      
	      //set Output value class
	      job.setOutputValueClass(LongWritable.class);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      return job.waitForCompletion(true) ? 0 : -1;
	   }

	   public static void main(String[] args) throws Exception {
	      System.exit(ToolRunner.run(new ColorCount(), args));
	   }
	}
