package airBNB;

import Tri.IntWritableInverseComparator;
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
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class CheapestHouses {
	private static final String INPUT_PATH = "input-AirBNB/";
	private static final String OUTPUT_PATH = "output/AirBNB-";
	private static final Logger LOG = Logger.getLogger(CheapestHouses.class.getName());
	//0  id,
	//1  name,
	//2  host_id,
	//3  host_name,
	//4  neighbourhood_group,
	//5  neighbourhood,
	//6  latitude,
	//7  longitude,
	//8  room_type,
	//9  price,
	//10 minimum_nights,
	//11 number_of_reviews,
	//12 last_review,
	//13 reviews_per_month,
	//14 calculated_host_listings_count,
	//15 availability_365
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

            int hostId = Integer.parseInt(tokens[HOST_ID_INDEX]);
            String hostName = tokens[HOST_NAME_INDEX];
            String roomType = tokens[ROOM_TYPE_INDEX];
            long price = (long) Float.parseFloat(tokens[PRICE_INDEX]);
            int minimumNights = Integer.parseInt(tokens[MINIMUM_NIGHT_INDEX]);
            int nbReviews = Integer.parseInt(tokens[NB_REVIEWS_INDEX]);

            context.write(new Text( hostId + ";" + hostName ), one);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {


        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;

            for (IntWritable value : values){
                count ++;
            }

            context.write(key, new IntWritable(count));

        }

    }

    public static class SortMap extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//            System.out.println("Mapmap : " + value);
            String[] tokens = value.toString().split("\t");
            String host= tokens[0];
            int offers = Integer.parseInt(tokens[1]);

            context.write(new IntWritable(offers), new Text(host));
        }
    }

    public static class SortReduce extends Reducer<IntWritable, Text, Text, IntWritable> {


        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values){

//                System.out.println("Reducing " + key + " ==> " + value);
                context.write(value, key);
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

		//////////////////////////////////

        Job sortJob = new Job(conf, "BestOffers");

        sortJob.setOutputKeyClass(IntWritable.class);
        sortJob.setOutputValueClass(Text.class);

        sortJob.setSortComparatorClass(IntWritableInverseComparator.class);

        sortJob.setMapperClass(SortMap.class);
        sortJob.setReducerClass(SortReduce.class);

        sortJob.setInputFormatClass(TextInputFormat.class);
        sortJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(sortJob, outPath);
        FileOutputFormat.setOutputPath(sortJob, new Path(OUTPUT_PATH + "-" + MethodHandles.lookup().lookupClass().getSimpleName() + Instant.now().getEpochSecond()));
        sortJob.waitForCompletion(true);
	}
}