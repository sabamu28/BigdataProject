package com.bd.project.stripe;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripeApproachReducer extends Reducer<Text, MapWritable, Text, Text> {
	double total = 0.0;

	@Override
	public void reduce(Text key, Iterable<MapWritable> stripeMaps, Context context) throws IOException, InterruptedException {
		MapWritable stripeMap = new MapWritable();
		total = 0;

		for (MapWritable map : stripeMaps) {
			Set<Writable> mapSet = map.keySet();

			for (Writable item : mapSet) {
				if (stripeMap.containsKey(item)) {
					stripeMap.put(item, new IntWritable(((IntWritable) stripeMap.get(item)).get() + 1));
				} else{
					stripeMap.put(item, map.get(item));
				}
				
				total += ((IntWritable) map.get(item)).get();
			}
		}
	

		Set<Writable> stripeKeys = stripeMap.keySet();

		for (Writable stripeKey : stripeKeys) {
			stripeMap.put(stripeKey,
					new DoubleWritable(((IntWritable) stripeMap.get(stripeKey)).get() / (double) total));
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append("[");

		Iterator<Entry<Writable, Writable>> it = stripeMap.entrySet().iterator();
		while (it.hasNext()) {
			MapWritable.Entry<Writable, Writable> entry = (MapWritable.Entry<Writable, Writable>) it.next();
			sb.append("(");
			sb.append(entry.getKey().toString());
			sb.append(", ");
			sb.append(((DoubleWritable) entry.getValue()).get());
			sb.append(")");
		}

		sb.append("]");
		context.write(key, new Text(sb.toString()));

	}
}
