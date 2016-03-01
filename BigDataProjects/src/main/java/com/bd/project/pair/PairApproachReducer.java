package com.bd.project.pair;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.bd.project.utilities.Pair;



public class PairApproachReducer extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {
	int total = 0;

	@Override
	public void reduce(Pair key, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {
		if (key.getSecondPair().toString().equals("*")) {
			total = 0;
			for (IntWritable c : counts) {
				total += c.get();
			}
		} else {
			int sum = 0;
			for (IntWritable c : counts) {
				sum += c.get();
			}
			context.write(key, new DoubleWritable(sum / (double) total));
		}
	}

}
