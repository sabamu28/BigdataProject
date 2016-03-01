package com.bd.project.stripe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StripeApproachMain extends Configured {

	public static void main(String[] args) throws Exception {
		Job job = new Job(new Configuration());
		job.setJarByClass(StripeApproachMain.class);

		Path outputDir = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputDir);

		FileSystem hdfs = FileSystem.get(job.getConfiguration());
		if (hdfs.exists(outputDir)) {
			hdfs.delete(outputDir, true);
		}

		job.setMapperClass(StripeApproachMapper.class);
		job.setReducerClass(StripeApproachReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		job.waitForCompletion(true);
	
	}

}

