package PackageDemo;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.text.SimpleDateFormat;
import java.util.Date;

public class Log_File {
	
	  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, IntWritable>{
	  private final static IntWritable one = new IntWritable(1);
	  private Text word = new Text();	
	  
	  
	  public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] words=line.split(",");
		  String Date_Start = words[1];
		  String Date_Stop = words[2];
		  SimpleDateFormat format = new SimpleDateFormat(words[1]);
		  
		  Date d1 = null;
		  Date d2 = null;
		  
		  try {
				d1 = format.parse(Date_Start);
				d2 = format.parse(Date_Stop);
				long diff = d2.getTime() - d1.getTime();

				long diffSeconds = diff / 1000 % 60;
				long diffMinutes = diff / (60 * 1000) % 60;
				long diffHours = diff / (60 * 60 * 1000) % 24;
				long diffDays = diff / (24 * 60 * 60 * 1000);

	
				System.out.print(diffMinutes + " minutes, ");
				System.out.print(diffDays + " days, ");
				System.out.print(diffHours + " hours, ");
				System.out.print(diffMinutes + " minutes, ");
				System.out.print(diffSeconds + " seconds.");

				

		} 
		  catch (Exception e) {
				e.printStackTrace();
		}
		  IntWritable outputValue = new IntWritable(diffMinutes);
		  context.write(words[0], );

	  }  
   }
}





















