package com.bd.project.pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.bd.project.utilities.Pair;

public class PairApproachMapper extends Mapper<LongWritable, Text, Pair, IntWritable> {

	Map<Pair, Integer> pairMap = new LinkedHashMap<Pair, Integer>();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String input = value.toString();
		String[] lines = input.split("\\n");
		for (int i = 0; i < lines.length; i++) {
			String[] item = lines[i].split("\\s");
			for (int j = 0; j < item.length - 1; j++) {
				for (int k = j + 1; k < item.length; k++) {
					if (!item[j].equals(item[k])) {
						Pair termPair = new Pair(item[j], item[k]);
						Pair starPair = new Pair(item[j], "*");
						if (pairMap.containsKey(starPair)) {
							if (starPair.getFirstPair().equalsIgnoreCase(item[j]))
								pairMap.put(starPair, pairMap.get(starPair) + 1);
						} else {
							pairMap.put(starPair, 1);
						}
						if (pairMap.containsKey(termPair)) {
							pairMap.put(termPair, pairMap.get(termPair) + 1);
						} else {
							pairMap.put(termPair, 1);
						}
					} else
						break; 
								
				}
			}
		}
		

	}
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Iterator<Entry<Pair, Integer>> it = pairMap.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<Pair, Integer> entry = (Map.Entry<Pair, Integer>) it.next();
			context.write(entry.getKey(), new IntWritable(entry.getValue()));
		}
	}

}

