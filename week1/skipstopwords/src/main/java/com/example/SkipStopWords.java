package com.example;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SkipStopWords extends Configured implements Tool{

   
   public static class SkipWordsMapper extends Mapper<Object, Text, Text, NullWritable> {
	   
	   private Set<String> stopWordList = new HashSet<String>();
	   private Text word = new Text();
	   
	   @SuppressWarnings("deprecation")
		protected void setup(Context context) throws java.io.IOException,
				InterruptedException {

			try {

				Path[] stopWordFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				//System.out.println(stopWordFiles.toString());
				if (stopWordFiles != null && stopWordFiles.length > 0) {
						readStopWordFile(stopWordFiles[0]);
				}
			} catch (IOException e) {
				System.err.println("Exception reading stop word file: " + e);

			}

	}
	   
	   private void readStopWordFile(Path stopWordFile) {
			try {
				BufferedReader  fis = new BufferedReader(new FileReader(stopWordFile.toString()));
				String stopWord = null;
				while ((stopWord = fis.readLine()) != null) {
					stopWordList.add(stopWord);
				}
			} catch (IOException ioe) {
				System.err.println("Exception while reading stop word file '"
						+ stopWordFile + "' : " + ioe.toString());
			}
	   }
	   
	   
	   public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				char lastchar = token.charAt(token.length()-1);
				if(lastchar == ',' || lastchar == '.') token = token.substring(0, token.length()-1);
				if (!stopWordList.contains(token.toLowerCase())) {
					word.set(token);
					context.write(word, null);
				}
			}
	}
	   
	   
	   
   }
   
   
   public int run(String[] args) throws Exception {
/*	   if (args.length <7) {
			System.err.printf("Usage: hadoop jar target/... -skip stopwords.txt\n",
					getClass().getSimpleName());
			return -1;
		}
	   
		*/
       Configuration conf = getConf();

       args = new GenericOptionsParser(conf, args).getRemainingArgs();

       Job job = Job.getInstance(conf);

       job.setJarByClass(SkipStopWords.class);
       job.setMapperClass(SkipWordsMapper.class);
       //job.setReducerClass(TopTenReducer.class);
       //job.setOutputKeyClass(NullWritable.class);
      // job.setOutputValueClass(Text.class);
       job.setNumReduceTasks(0);
       
       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
       
       DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());

       return (job.waitForCompletion(true) ? 0 : 1);
   }

   public static void main(String[] args) throws Exception {
       int exitCode = ToolRunner.run(new SkipStopWords(), args);
       System.exit(exitCode);
   }
}
