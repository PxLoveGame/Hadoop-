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

public class MoyensDePaiement {
	private static final String INPUT_PATH = "input-Taxis/";
	private static final String OUTPUT_PATH = "output/Taxis-";
	private static final Logger LOG = Logger.getLogger(MoyensDePaiement.class.getName());
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

	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			if (key.get() == 0) return;
			if (value.toString().equals("")) return;

//            System.out.println("Parsing " + value);
			
			String[] tokens = value.toString().split(",");

			int pickupDay = Integer.parseInt(tokens[PICKUP_TIME_INDEX].split(" ")[0].split("-")[2]); // 2018-01-01 00:21:05
            int passengerCount = Integer.parseInt(tokens[PASSENGER_COUNT_INDEX]);
            int paymentType = Integer.parseInt(tokens[PAYMENT_TYPE_INDEX]);
            int PULocationId = Integer.parseInt(tokens[PU_LOCATION_ID_INDEX]);
            int DOLocationId = Integer.parseInt(tokens[DO_LOCATION_ID_INDEX]);
            long tip = (long) Float.parseFloat(tokens[TIP_INDEX]);
            long totalFee = (long) Float.parseFloat(tokens[TOTAL_FEE_INDEX]);


			context.write(new IntWritable(pickupDay), new IntWritable(paymentType));
		}
	}

	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

	    private HashMap<Integer, Integer> paymentTypes= new HashMap<>();

		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

            for (int i = 1; i < 7; i++){
                paymentTypes.put(i, 0);
            }


		    for (IntWritable value : values){
                paymentTypes.put(value.get(), paymentTypes.get(value.get())+1);
            }


		    int maxOccurence = 0;
		    int maxOccurring = 1;


		    for (java.util.Map.Entry<Integer, Integer> entry : paymentTypes.entrySet()){

		        int paymentType = entry.getKey();
		        int occurrences = entry.getValue();

		        if (occurrences > maxOccurence){
		            maxOccurence = occurrences;
		            maxOccurring = paymentType;
                }
            }


		    String topPayment = PAYMENT_METHODS.get(maxOccurring);

		    context.write(key, new Text(topPayment));

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