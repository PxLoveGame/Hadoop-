package Taxis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class AvgTripPrice {
	private static final String INPUT_PATH = "input-Taxis/";
	private static final String OUTPUT_PATH = "output/Taxis-";
	private static final Logger LOG = Logger.getLogger(AvgTripPrice.class.getName());

	private static final int TOTAL_FEE_INDEX = 16;





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

	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			if (key.get() == 0) return;
			if (value.toString().equals("")) return;
			
			String[] tokens = value.toString().split(",");

            long totalFee = (long) Float.parseFloat(tokens[TOTAL_FEE_INDEX]);
			context.write(new Text("trip"), new LongWritable(totalFee));
		}
	}

	public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {


		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

		    long sum = 0;
		    long count = 0;

		    for (LongWritable value : values){
                sum += value.get();
                count++;
            }

			float avg = (float) sum / (float) count;

		    context.write(new Text("avg"), new Text((String.valueOf(avg))));

		}
    }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "TripPriceAvg");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + "-" + MethodHandles.lookup().lookupClass().getSimpleName() + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}