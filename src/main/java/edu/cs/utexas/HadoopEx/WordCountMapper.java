package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<Object, Text, Text, DoubleWritable> {

	private DoubleWritable counter = new DoubleWritable();
	private Text word = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			
		// diff code for task 1 versus task 2
		String currLine = value.toString(); 
		if (currLine.startsWith("YEAR")){
			return;
		}
		String[] lineParts = currLine.split(","); // split line by commas

		// check for enough parts to get delay and delay valid for task 2
		if (lineParts.length > 11 && !lineParts[11].isEmpty()) { 
			// pulls the airline and the delay and writes it to context for task 2
			word.set(lineParts[4]);
			counter.set(Double.parseDouble(lineParts[11]));
			// writes airline and delay to context for task 2
			context.write(word, counter);
		}
		
		// task 1 code
		// if (lineParts.length > 7){
			// get the airline and write it to context for task 1
			// 	word.set(lineParts[7]);
			// 	context.write(word, counter);
		// }
	}
}