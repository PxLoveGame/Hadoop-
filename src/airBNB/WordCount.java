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
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class WordCount {
	private static final String INPUT_PATH = "input-AirBNB/";
	private static final String OUTPUT_PATH = "output/AirBNB-";
	private static final Logger LOG = Logger.getLogger(WordCount.class.getName());

    private static final int OFFER_ID_INDEX = 0;
    private static final int DESCRIPTION_INDEX = 1;
	private static final int HOST_ID_INDEX = 2;
	private static final int HOST_NAME_INDEX = 3;
    private static final int NEIGHBOURHOOD_INDEX = 5;
    private static final int LAT_INDEX = 6;
    private static final int LON_INDEX = 7;
	private static final int ROOM_TYPE_INDEX = 8;
	private static final int PRICE_INDEX = 9;
	private static final int MINIMUM_NIGHT_INDEX = 10;
	private static final int NB_REVIEWS_INDEX = 11;
	private static final int VALID_TOKENS_LENGTH = 16;
	private static final IntWritable one = new IntWritable(1);

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



    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (key.get() == 0) return;
            if (value.toString().equals("")) return;

            String[] tokens = value.toString().split(",(\"[^\"]*\")?"); // match ',' not enclosed by '"'


            if (tokens.length != VALID_TOKENS_LENGTH) return; // erreur de parsing Hadoop (typiquement, la description contient un \n)

            try {

                String description = tokens[DESCRIPTION_INDEX].toLowerCase();
                description = description.replaceAll("[^\\x00-\\x7F]", ""); // remove non ASCII
                description = description.replaceAll("\\.|'|!|\\?|,|:|‘|;|\"|\\(|\\)|\\d*", ""); // ponctuation, chiffres etc.
                String[] words = description.split("\\s+");
                for (String w : words){
                    if (w.length() > 0) {
                        context.write( new Text(w), one );
                    }
                }


            } catch (NumberFormatException e){
                LOG.severe("Error parsing line " + key + " : " + value);
                e.printStackTrace();
            }

        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            for (IntWritable ignored : values){
                count++;
            }

            System.out.println("Reduce " + key + " ==> " + count);

//            String[] tokens;
//            int count = 0;
//            float lat, lon;
//            float avgLat, avgLon;
//            float sumLat = 0, sumLon = 0;
//
//            for (Text value : values){
//                count++;
//                tokens = value.toString().split(";");
//                lat = Float.parseFloat(tokens[0]);
//                lon = Float.parseFloat(tokens[1]);
//
//                sumLat += lat;
//                sumLon += lon;
//            }
//
//            avgLat = sumLat / count;
//            avgLon = sumLon / count;
//
//            context.write(key, new Text( avgLat + "\t" + avgLon ));
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