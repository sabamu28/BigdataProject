package com.bd.project.hybrid;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.bd.project.utilities.Pair;



public class HybridApproachReducer extends Reducer<Pair, IntWritable, Text, Text> {

	private Map<String, Double> H = new HashMap<String, Double>();
	double marginal = 0.0;

	String currentTerm = null;

	@Override
	public void reduce(Pair key, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {
		if (currentTerm == null)
			currentTerm = key.getFirstPair();
		else if (!currentTerm.equalsIgnoreCase(key.getFirstPair())) {
			Set<String> HKeys = H.keySet();
			for (String HKey : HKeys) {
				H.put(HKey, (H.get(HKey) / marginal));
			}

			StringBuilder sb = new StringBuilder();
			sb.append("[");
			Iterator<Entry<String, Double>> it = H.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, Double> entry = it.next();
				sb.append("(");
				sb.append(entry.getKey());
				sb.append(", ");
				sb.append(entry.getValue());
				sb.append(")");
			}

			sb.append("]");
			context.write(new Text(currentTerm), new Text(sb.toString()));

			// reset for new item
			marginal = 0;
			H = new HashMap<String,Double>();
			currentTerm = key.getFirstPair();
		}
		int sum = 0;
		for (IntWritable c : counts) {
			sum += c.get();
		}
		if(H.containsKey(key.getSecondPair()))
			H.put(key.getSecondPair(), H.get(key.getSecondPair()) + sum);
		else
			H.put(key.getSecondPair(),0.0 +sum);
		marginal += sum;

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Set<String> HKeys = H.keySet();
		for (String HKey : HKeys) {
			H.put(HKey, (H.get(HKey) / marginal));
		}

		StringBuilder sb = new StringBuilder();
		sb.append("[");
		Iterator<Entry<String, Double>> it = H.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Double> entry = it.next();
			sb.append("(");
			sb.append(entry.getKey());
			sb.append(", ");
			sb.append(entry.getValue());
			sb.append(")");
		}

		sb.append("]");
		context.write(new Text(currentTerm), new Text(sb.toString()));
	}

}
