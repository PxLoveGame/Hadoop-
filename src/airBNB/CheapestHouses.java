package airBNB;

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
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class CheapestHouses {
	private static final String INPUT_PATH = "input-AirBNB/";
	private static final String OUTPUT_PATH = "output/AirBNB-";
	private static final Logger LOG = Logger.getLogger(CheapestHouses.class.getName());

    private static final int OFFER_ID_INDEX = 0;
	private static final int ROOM_TYPE_INDEX = 8;
	private static final int PRICE_INDEX = 9;
	private static final int VALID_TOKENS_LENGTH = 16;

	private static int TOP_LIMITER = 10; // k
	
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



    public static class Map extends Mapper<LongWritable, Text, LongWritable, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (key.get() == 0) return;
            if (value.toString().equals("")) return;

            String[] tokens = value.toString().split(",(\"[^\"]*\")?"); // match ',' not enclosed by '"'

            if (tokens.length != VALID_TOKENS_LENGTH) return; // erreur de parsing Hadoop (typiquement, la description contient un \n)

            try {
                int offerId = Integer.parseInt(tokens[OFFER_ID_INDEX]);

                long price = (long) Float.parseFloat(tokens[PRICE_INDEX]);

                String roomType = tokens[ROOM_TYPE_INDEX];


                if (roomType.equals("Entire home/apt")) {
                    context.write(new LongWritable(price), new IntWritable( offerId ));
                }
            } catch (NumberFormatException e){
                LOG.severe("Error parsing line " + key + " : " + value);
                e.printStackTrace();
            }

        }
    }

    public static class Reduce extends Reducer<LongWritable, IntWritable, IntWritable, LongWritable> {


	    TreeMap<Long, List<Integer>> cheapest = new TreeMap<>();

        @Override
        public void reduce(LongWritable price, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            if (!cheapest.containsKey(price.get())){
                cheapest.put(price.get(), new ArrayList<>());
            }

            for (IntWritable offerId : values){
                if (cheapest.size() < TOP_LIMITER){
                    cheapest.get(price.get()).add(offerId.get());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int count = 0;

            for ( Long price : cheapest.keySet() ){
                for (int offerId : cheapest.get(price)) {
                    if (count < TOP_LIMITER){
                        count++;
                        context.write(new IntWritable(offerId), new LongWritable(price));
                    }else break;
                }
            }

        }

    }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "CountOffers");

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		Path outPath = new Path(OUTPUT_PATH + "-" + MethodHandles.lookup().lookupClass().getSimpleName() + Instant.now().getEpochSecond());
		FileOutputFormat.setOutputPath(job, outPath);

		job.waitForCompletion(true);

	}
}