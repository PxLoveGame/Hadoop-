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
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class OfferSpreading {
	private static final String INPUT_PATH = "input-AirBNB/";
	private static final String OUTPUT_PATH = "output/AirBNB-";
	private static final Logger LOG = Logger.getLogger(OfferSpreading.class.getName());

    private static final int OFFER_ID_INDEX = 0;
	private static final int HOST_ID_INDEX = 2;
	private static final int HOST_NAME_INDEX = 3;
	private static final int ROOM_TYPE_INDEX = 8;
	private static final int PRICE_INDEX = 9;
	private static final int MINIMUM_NIGHT_INDEX = 10;
	private static final int NB_REVIEWS_INDEX = 11;
	private static final int VALID_TOKENS_LENGTH = 16;

	private static final IntWritable one = new IntWritable(1);







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



    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

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

                context.write(new Text(roomType), one);

            } catch (NumberFormatException e){
                LOG.severe("Error parsing line " + key + " : " + value);
                e.printStackTrace();
            }

        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {


	    HashMap<String, Integer> repartition = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {


            int count = 0;

            for (IntWritable ignored : values){
                count++;
            }

            repartition.put(key.toString(), count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            float sum = 0;


            for (Integer v : repartition.values()){
                sum += v;
            }

            for (String type : repartition.keySet()){
                float count = repartition.get(type);
                float ratio = (count * (float) 100) / sum;

                context.write(new Text(type), new Text(String.valueOf(ratio)));
            }


        }

    }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "CountOffers");

		job.setOutputKeyClass(Text.class);
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