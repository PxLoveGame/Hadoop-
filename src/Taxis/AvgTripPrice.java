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
import java.util.List;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class AvgTripPrice {
	private static final String INPUT_PATH = "input-Taxis/";
	private static final String OUTPUT_PATH = "output/Taxis-";
	private static final Logger LOG = Logger.getLogger(AvgTripPrice.class.getName());
    //0  VendorID,
    //1  tpep_pickup_datetime,
    //2  tpep_dropoff_datetime,
    //3  passenger_count,
    //4  trip_distance,
    //5  RatecodeID,
    //6  store_and_fwd_flag,
    //7  PULocationID,
    //8  DOLocationID,
    //9  payment_type,
    //10 fare_amount,
    //11 extra,
    //12 mta_tax,
    //13 tip_amount,
    //14 tolls_amount,
    //15 improvement_surcharge,
    //16 total_amount
	private static final int PICKUP_TIME_INDEX = 1;
	private static final int PASSENGER_COUNT_INDEX = 3;
	private static final int PAYMENT_TYPE_INDEX = 9;
	private static final int PU_LOCATION_ID_INDEX = 7;
	private static final int DO_LOCATION_ID_INDEX = 8;
	private static final int TIP_INDEX = 13;
	private static final int TOTAL_FEE_INDEX = 16;
	private static final int TRIP_DISTANCE_INDEX = 4;




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

//            System.out.println("Parsing " + value);
			
			String[] tokens = value.toString().split(",");


            long totalFee = (long) Float.parseFloat(tokens[TOTAL_FEE_INDEX]);


			context.write(new Text("trip"), new LongWritable(totalFee));
//            System.out.println("Mapping " + pickupHour + " ==> " + passengerCount);
		}
	}

	public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {

	    private TreeMap<Integer, List<Integer>> influence = new TreeMap<>();

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