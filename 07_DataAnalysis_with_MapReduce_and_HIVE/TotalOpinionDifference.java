import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalOpinionDifference {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		private Text imdb_title_id = new Text();
		private DoubleWritable opinion_difference = new DoubleWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(",");
			{

				try {
					float males_allages_avg_vote = Float.parseFloat(tokens[5]);
					float females_allages_avg_vote = Float.parseFloat(tokens[6]);

					float avg_diference = Math.abs(males_allages_avg_vote-females_allages_avg_vote); //difference rating
					
					imdb_title_id.set(tokens[0]);
					opinion_difference.set(avg_diference);

					context.write(imdb_title_id, opinion_difference);

				} catch (Exception ex) {
					System.out.println(imdb_title_id);
					System.out.println(ex.toString());
				}

			}
		}
	}

	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {

			double sum = 0;
			int count = 0;

			for (DoubleWritable val : values) {
				sum = sum + val.get();
				count++;
			}
			double rate_dif_avg = 0;
			rate_dif_avg = sum / count;
			context.write(key, new DoubleWritable(rate_dif_avg));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "TotalOpinionDifference");
		job.setJarByClass(TotalOpinionDifference.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}