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

public class WordCount {
	private static final String INPUT_PATH = "input-AirBNB/";
	private static final String OUTPUT_PATH = "output/AirBNB-";
	private static final Logger LOG = Logger.getLogger(WordCount.class.getName());

    private static final int DESCRIPTION_INDEX = 1;
	private static final int VALID_TOKENS_LENGTH = 16;
	private static final IntWritable one = new IntWritable(1);

    private static final int MIN_OCCURENCES = 100;




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
                description = description.replaceAll("[^a-zA-Z]", " "); // remove non ASCII
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

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {


        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            for (IntWritable ignored : values){
                count++;
            }


            if (count > MIN_OCCURENCES) {
                context.write(key, new IntWritable(count));
            }
        }

    }


    public static class SortMap extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = value.toString().split("\t");
            int count = Integer.parseInt(tokens[1]);
            String word = tokens[0];

            context.write(new IntWritable(count), new Text(word));

        }
    }

    public static class SortReduce extends Reducer<IntWritable, Text, Text, IntWritable> {


        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                context.write(value, key);
            }
        }

    }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

        Job job = new Job(conf, "CountWords");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        Path outPath = new Path( OUTPUT_PATH + "-" + MethodHandles.lookup().lookupClass().getSimpleName() + Instant.now().getEpochSecond() );
        FileOutputFormat.setOutputPath(job, outPath);

        job.waitForCompletion(true);

        /////////////////////////////////////////////////


        Job sortJob = new Job(conf, "sort");

        sortJob.setOutputKeyClass(IntWritable.class);
        sortJob.setOutputValueClass(Text.class);

        sortJob.setSortComparatorClass(IntWritableInverseComparator.class);

        sortJob.setMapperClass(SortMap.class);
        sortJob.setReducerClass(SortReduce.class);

        sortJob.setInputFormatClass(TextInputFormat.class);
        sortJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(sortJob, outPath);
        outPath = new Path( OUTPUT_PATH + "-" + MethodHandles.lookup().lookupClass().getSimpleName() + Instant.now().getEpochSecond() );
        FileOutputFormat.setOutputPath(sortJob, outPath);

        sortJob.waitForCompletion(true);

	}
}