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
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



public class Rank extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger(Rank.class);
public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Rank(), args);
      System .exit(res);
   }


   public int run( String[] args) throws  Exception {

     


//conf.set("query", new String(sb.substring(0,sb.length()-1)));
 Job job  = Job .getInstance(getConf(), "job1");
      job.setJarByClass( this .getClass());
      

      FileInputFormat.addInputPaths(job,  args[0]);

      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
job.setMapOutputKeyClass(DoubleWritable.class);
job.setMapOutputValueClass(Text.class);
job.setSortComparatorClass(MyKeyComparator.class);
 //job.setSortComparatorClass(DoubleWritable.DecreasingComparator.class);


      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass(DoubleWritable .class);
//job.waitForCompletion(true);


     return job.waitForCompletion( true)  ? 0 : 1;


   }
public static class MyKeyComparator extends WritableComparator {
    protected MyKeyComparator() {
          super(DoubleWritable.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleWritable key1 = (DoubleWritable) w1;
        DoubleWritable key2 = (DoubleWritable) w2;          
        return -1 * key1.compareTo(key2);
    }
}

   
   public static class Map extends Mapper<LongWritable, Text,  DoubleWritable,  Text > {
    //  private final static IntWritable one  = new IntWritable( 1);
    //  private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\B*\\s*");

      public void map(LongWritable offset, Text lineText, Context context)
        throws  IOException,  InterruptedException {
 //int  input_size= (int) context.getConfiguration().getLong("mapred.map.tasks",1);

         String line  = lineText.toString();
                    String[] words=line.split("\\s+");
if(words.length>=0)
{
   //words[1]+"##"+
context.write(new DoubleWritable(Double.parseDouble(words[1])),new Text(words[0]));
   


}
        // Text currentWord  = new Text();

      /*   for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
           // currentWord  = new Text(word+"#####"+((FileSplit)context.getInputSplit()).getPath().getName()+"#####"+String.valueOf(input_size));
          
         }  */
      }
   }

   public static class Reduce extends Reducer<DoubleWritable ,  Text ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce(DoubleWritable word,  Iterable<Text> values,  Context context)
         throws IOException,  InterruptedException {
//String key=word.toString();
//String key_value[]=key.split("##");

//if(key_value.length>0)
//{
Text count=new Text();
       for(Text rank:values)
{
//count=rank;
context.write(rank,word);
}
//new Text(String.valueOf(key_value[0]))

//}
        
         
      }
   }
}



