package com.bd.project.stripe;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StripeApproach extends Configured implements Tool {
	/**
	 * 
	 * @param args
	 * Relative frequency: Stripe approach
	class Mapper
	method Map(docid a; doc d)
		for all term w in doc d do
		H = new AssociativeArray
		for all term u in Neighbors(w) do
		H{u} = H{u} + 1 . //Tally words co-occurring with w Emit(Term w; Stripe H)
	class Reducer
	method Reduce(term w; stripes [H1;H2;H3; : : :])
		Hf = new AssociativeArray
		for all stripe H in stripes [H1;H2;H3; …] do
		Sum(Hf ; H) . //Element-wise sum
		total = 0
		for all term u in Hf
		total = tota + Hf(u)
		for all term u in Hf
		Hf(u) = Hf(u)/total
		Emit(term w; stripe Hf )
	//other apporoach
	total =0
	total += sum(Hf,H)
	for all term u in Hf
	H(f) = Hf(u)/total;
	 */
	
	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] doc = line.split("\\s"); //split with space
			for(int i=0;i<doc.length-1;i++){
				Text w = new Text(doc[i]);
				MapWritable stripeMap = new MapWritable();
				for(int j=i+1; j<doc.length; j++){
					Text x= new Text(doc[j]);
					if(w.equals(x))
						break;
					
					if(!stripeMap.containsKey(x)){
						stripeMap.put(x, new IntWritable(1));
					}else{
						stripeMap.put(x, new IntWritable(((IntWritable)stripeMap.get(x)).get() + 1));
					}
										
				}
				context.write(w, stripeMap);
				
			}
					
		}
	}
	
	
	public static class Reduce extends Reducer<Text, MapWritable, Text, Text> {
		double total = 0.0;

		@Override
		public void reduce(Text key, Iterable<MapWritable> stripeMaps, Context context) throws IOException, InterruptedException {
			MapWritable stripeMap = new MapWritable();
			total = 0;

			for (MapWritable map : stripeMaps) {
				Set<Writable> mapSet = map.keySet();

				for (Writable item : mapSet) {
					if (stripeMap.containsKey(item)) {
						stripeMap.put(item, new IntWritable(((IntWritable) map.get(item)).get() + 1));
					} else
						stripeMap.put(item, map.get(item));

					total += ((IntWritable) map.get(item)).get();
				}
			}

			Set<Writable> stripeKeys = stripeMap.keySet();

			for (Writable stripeKey : stripeKeys) {
				stripeMap.put(stripeKey,
						new DoubleWritable(((IntWritable) stripeMap.get(stripeKey)).get() / (double) total));
			}
			// context.write(key, stripeMap);

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
	

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (!(args.length == 2)) {
			System.out.printf("Usage: StripeApproach <input dir> <output dir>\n");
			System.exit(-1);
		}

		int appRunner = ToolRunner.run(new StripeApproach(), args);
		System.exit(appRunner);

	}
	
	public int run(String[] args) throws Exception {
		Job job = new Job(new Configuration());
		job.setJarByClass(this.getClass());

		Path outputDir = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputDir);

		FileSystem hdfs = FileSystem.get(getConf());
		if (hdfs.exists(outputDir)) {
			hdfs.delete(outputDir, true);
		}

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}


}
