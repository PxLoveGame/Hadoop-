package Taxis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class HeuresPointe {
	private static final String INPUT_PATH = "input-Taxis/";
	private static final String OUTPUT_PATH = "output/Taxis-";
	private static final Logger LOG = Logger.getLogger(HeuresPointe.class.getName());
	private static final int PICKUP_TIME_INDEX = 1;
	private static final int PASSENGER_COUNT_INDEX = 3;
	
	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

		try {
			FileHandler fh = new FileHandler("out.log");
			fh.setFormatter(new SimpleFormatter());
			LOG.addHandler(fh);
		} catch (SecurityException | IOException e) {
			System.exit(1);
		}
	}

	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			if (key.get() == 0) return;
			if (value.toString().equals("")) return;
			
			String[] tokens = value.toString().split(",");

			int pickupHour = Integer.parseInt(tokens[PICKUP_TIME_INDEX].split(" ")[1].split(":")[0]); // 2018-01-01 00:21:05
            int passengerCount = Integer.parseInt(tokens[PASSENGER_COUNT_INDEX]);
            context.write(new IntWritable(pickupHour), new IntWritable(passengerCount));

		}
	}

	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	    private TreeMap<Integer, List<Integer>> influence = new TreeMap<>();

		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

		    int count = 0;

		    for (IntWritable value : values){
                count += value.get();
            }

            if ( !influence.containsKey(count) ){
                influence.put(count, new ArrayList<>());
            }

            influence.get(count).add(key.get());

		}

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

		    ArrayList<Integer> values = new ArrayList<>();
		    values.addAll(influence.keySet());
            Collections.reverse(values);

		    for (Integer value : values){
		        for (Integer key : influence.get(value) ){
                    context.write(new IntWritable(key), new IntWritable(value));
                }

            }
        }
    }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "DistinctCustomers");

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + "-" + MethodHandles.lookup().lookupClass().getSimpleName() + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}