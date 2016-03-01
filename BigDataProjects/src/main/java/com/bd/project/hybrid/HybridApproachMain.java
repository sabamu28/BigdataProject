package com.bd.project.hybrid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bd.project.utilities.Pair;


public class HybridApproachMain extends Configured {

	public static void main(String[] args) throws Exception {
		Job job = new Job(new Configuration());
		job.setJarByClass(HybridApproachMain.class);

		Path outputDir = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputDir);

		FileSystem hdfs = FileSystem.get(job.getConfiguration());
		if (hdfs.exists(outputDir)) {
			hdfs.delete(outputDir, true);
		}

		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(HybridApproachMapper.class);
		job.setReducerClass(HybridApproachReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		job.waitForCompletion(true);
	}


}
