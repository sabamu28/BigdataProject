package com.bd.project.stripe;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class StripeApproachMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String input = value.toString();
		String[] lines = input.split("\\n");

		for (int i = 0; i < lines.length; i++) {
			String[] item = lines[i].split("\\s");
			for (int j = 0; j < item.length - 1; j++) {
				Text value1 = new Text(item[j]);
				MapWritable stripeMap = new MapWritable();
				for (int k = j + 1; k < item.length; k++) {
					Text value2 = new Text(item[k]);
					if (!value1.equals(value2)) {
						if (!stripeMap.containsKey(value2)) {
							stripeMap.put(value2, new IntWritable(1));
						} else {
							stripeMap.put(value2, new IntWritable(((IntWritable) stripeMap.get(value2)).get() + 1));
						}
					} else
						break;
				}
				context.write(new Text(item[j]), stripeMap);
			}
		}
	}

}
