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

public class ZonesDemandees {
	private static final String INPUT_PATH = "input-Taxis/";
	private static final String OUTPUT_PATH = "output/Taxis-";
	private static final Logger LOG = Logger.getLogger(ZonesDemandees.class.getName());

	private static final int PU_LOCATION_ID_INDEX = 7;


	private static final HashMap<Integer, String> PAYMENT_METHODS = new HashMap<>();


	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

		PAYMENT_METHODS.put(1, "Credit Card");
		PAYMENT_METHODS.put(2, "Cash");
		PAYMENT_METHODS.put(3, "No charge");
		PAYMENT_METHODS.put(4, "Dispute");
		PAYMENT_METHODS.put(5, "Unknown");
		PAYMENT_METHODS.put(6, "Voided Trip");

		try {
			FileHandler fh = new FileHandler("out.log");
			fh.setFormatter(new SimpleFormatter());
			LOG.addHandler(fh);
		} catch (SecurityException | IOException e) {
			System.exit(1);
		}
	}

	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			if (key.get() == 0) return;
			if (value.toString().equals("")) return;

            IntWritable k = null;

            String v = "";

			
			String[] tokens = value.toString().split(",");

			if (tokens.length == 4){ // lookup table zones

			    k = new IntWritable( Integer.parseInt(tokens[0]) );

			    v = "name;" + tokens[2]; // type : name

            }else { // trip data

                int PULocationId = Integer.parseInt(tokens[PU_LOCATION_ID_INDEX]);
                
                k = new IntWritable(PULocationId);
                v = "occu;1";
            }
            context.write(k, new Text(v));
        }
	}

	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {

	    private HashMap<Integer, String> zoneNames= new HashMap<>();
	    private TreeMap<Integer, List<Integer>> zoneCount= new TreeMap<>(Collections.reverseOrder());

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

            int count = 0;
            String name = null;
            String[] tokens;

            for (Text value : values){

                tokens = value.toString().split(";");
                if (tokens[0].equals("name")){
                    name = tokens[1];
                }else {
                    count++;
                }
            }
            zoneNames.put(key.get(), name);

            if (!zoneCount.containsKey(count)){
                zoneCount.put(count, new ArrayList<>());
            }
            zoneCount.get(count).add(key.get());

		}

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {


		    for (Integer count : zoneCount.keySet()){
		        for (Integer zoneId : zoneCount.get(count)){
		            IntWritable id = new IntWritable(zoneId);
		            String name = zoneNames.get(zoneId);
		            Text value = new Text( name +"\t" + count);

		            context.write(id, value);
                }
		    }
        }

    }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "DistinctCustomers");

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + "-" + MethodHandles.lookup().lookupClass().getSimpleName() + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}