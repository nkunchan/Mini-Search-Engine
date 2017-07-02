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

public class Search extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Search.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Search(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		StringBuffer sb = new StringBuffer();
               // Append all the terms of given search query in the argument to stringbuffer with "," delimiter 
		if (args.length >= 3) {
			for (int i = 2; i < args.length; i++) {
				sb.append(args[i]);
				sb.append(",");
			}

		}
		//Set the search query values to some value in conf here for eg. "query" using conf.set 
		conf.set("query", new String(sb.substring(0, sb.length() - 1)));
		Job job = Job.getInstance(conf, "job1");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// set key and value class for mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// set key and value class for job output which is reduce
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private static final Pattern WORD_BOUNDARY = Pattern
				.compile("\\s*\\B*\\s*");
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
                        // extract the configuration setting to get the value of search query.
			Configuration conf = context.getConfiguration();
			String param = conf.get("query");
			String line = lineText.toString();
			// split the output of TFIDF using spaces to fectch file name and tfidf values 
			String[] words = line.split("\\s+");
			if (words.length >= 2) {
			// split the first term to get word and file name
				String[] word_file = words[0].split("#####");
				if (word_file.length == 2) {
					// compare the word with search query terms one by one, if it matches, send the file name  and tfidf value to reduce 
					for (String query_term : param.split(","))
						if (word_file[0].compareToIgnoreCase(query_term) == 0)
							context.write(new Text(word_file[1]), new Text(
									words[1]));

				}

			}
		}
	}

	public static class Reduce extends
			Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<Text> counts, Context context)
				throws IOException, InterruptedException {
			double freq_sum = 0.0;
			// add the values of tfidf for the same file name and print filename and sum of tfidf values
			for (Text value : counts) {
				freq_sum = freq_sum + Double.parseDouble(value.toString());
			}
			context.write(word, new DoubleWritable(freq_sum));
		}
	}
}

