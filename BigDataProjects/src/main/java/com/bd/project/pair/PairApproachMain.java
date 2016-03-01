package com.bd.project.pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bd.project.utilities.Pair;


public class PairApproachMain extends Configured {

	public static void main(String[] args) throws Exception {
		Job job = new Job(new Configuration());
		job.setJarByClass(PairApproachMain.class);

		Path outputDir = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputDir);

		FileSystem hdfs = FileSystem.get(job.getConfiguration());
		if (hdfs.exists(outputDir)) {
			hdfs.delete(outputDir, true);
		}

		job.setMapperClass(PairApproachMapper.class);
		job.setReducerClass(PairApproachReducer.class);

		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}

}
