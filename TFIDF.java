// Name : Nikita Kunchanwar  Email id: nkunchan@uncc.edu

package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.lang.*;
import java.util.HashMap;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);
	// set the intermediate output path where 1st job values would be written 
	private static final String OUTPUT_PATH = "intermediate_output5";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TFIDF(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), "job1");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, args[0]);
		// set the output path of 1st job to some intermediate path
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce1.class);
		// set the 1st job's mapper key and value class 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// set the 1st job's reducer key and value class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.waitForCompletion(true);

		Job job2 = Job.getInstance(getConf(), "job2");
		job2.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job2, OUTPUT_PATH);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		// Set input format for second job explicitly as map of second job accepts text instead of default value which is LongWritable
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		// Set the 2nd job's mapper key and value class
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		// Set the 2nd job's reducer key and value class
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		return job2.waitForCompletion(true) ? 0 : 1;

	}

	public static class Map1 extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private static final Pattern WORD_BOUNDARY = Pattern
				.compile("\\s*\\b\\s*");
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			// get the size of input files provided to 1st job using mapred.map.tasks setting
			int input_size = (int) context.getConfiguration().getLong(
					"mapred.map.tasks", 1);
			String line = lineText.toString();
			Text currentWord = new Text();

			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
			// send the key as combination of word (lower case), file name and no of input files provided
			// and value as "one" 
				currentWord = new Text(word.toLowerCase()
						+ "#####"
						+ ((FileSplit) context.getInputSplit()).getPath()
								.getName() + "#####"
						+ String.valueOf(input_size));
				context.write(currentWord, one);
			}
		}
	}

	public static class Reduce1 extends
			Reducer<Text, IntWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			// calculate count for each word given to reducer
			for (IntWritable count : counts) {
				sum += count.get();
			}
			// calculate term frequency using logarithmic equation if count is greater than 0 and send it to 2nd job along with word got from 1st job as key
			double temp;
			if (sum > 0)
				temp = (1 + Math.log10(sum));
			else
				temp = 0;
			context.write(word, new DoubleWritable(temp));
		}
	}
	// Map and reduce for 2nd job to calculate the tfidf
	public static class Map2 extends Mapper<Text, Text, Text, Text> {

		private Text word = new Text();

		public void map(Text word, Text counts, Context context)
				throws IOException, InterruptedException {

			String line = word.toString();
			// split the key got from the 1st job to fetch file name and word
			String[] terms = line.split("#####");
			// combine the file name, termfrequency and no of input files as value along with word as key and send to reducer 
			if (terms.length == 3)
				context.write(new Text(terms[0]), new Text(terms[1] + "=" + counts.toString() + "=" + terms[2]));
		}
	}
	public static class Reduce2 extends
			Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<Text> counts, Context context)
				throws IOException, InterruptedException {
			int input_size = 0;
			double count = 0.0;
			// Maintain the hash map to store posting list
			HashMap<String, String> posting_entry = new HashMap<String, String>();
			for (Text entry : counts) {
				count++;
				String[] terms = entry.toString().split("=");
				if (terms.length >= 2) {
					posting_entry.put(terms[0], terms[1]);
					input_size = Integer.parseInt(terms[2]);

				}

			}
			double temp;
			// calculate tfidf using no of input documents and documents containing word
			temp = Math.log10(1 + (input_size / count));
			// fetch the values from hash map of posting list one by one and print along with calculated tfidf
			for (String key : posting_entry.keySet())
				context.write(new Text(word.toString() + "#####" + key),new DoubleWritable(temp*Double.parseDouble(posting_entry.get(key))));

		}
	}
}

