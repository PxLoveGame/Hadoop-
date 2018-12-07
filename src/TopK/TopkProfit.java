package TopK;

import Tri.LongWritableInverseComparator;
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
import java.time.Instant;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;



class ProfitMap extends Mapper<LongWritable, Text, LongWritable, Text> {

//	private int k;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) return; // retirer le header CSV

        String[] values = value.toString().split(",");

        long profit = (long) Float.parseFloat(values[20]);

        context.write(new LongWritable(profit), value);

    }
}

class ProfitReduce extends Reducer<LongWritable, Text, LongWritable, Text> {

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(key, value);
        }
    }
}

class SortMap extends Mapper<LongWritable, Text, LongWritable, Text> {

    private int k;
    @Override
    public void setup(Mapper.Context context) {
        k = context.getConfiguration().getInt("k", 1);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        long k = Long.parseLong(tokens[0]);
        Text v = new Text(tokens[1]);
        if (key.get() < k) {
            context.write(new LongWritable(k), new Text(v));
        }

    }
}

class SortReduce extends Reducer<LongWritable, Text, LongWritable, Text> {


    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, value);
        }
    }
}

public class TopkProfit {
	private static final String INPUT_PATH = "input-groupBy/";
	private static final String OUTPUT_PATH = "output/Topk-";
	private static final Logger LOG = Logger.getLogger(TopkProfit.class.getName());

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

	/**
	 * Ce programme permet le passage d'une valeur k en argument de la ligne de commande.
	 */
	public static void main(String[] args) throws Exception {
		// Borne 'k' du topk
		int k = 10;
		{

			try {
				// Passage du k en argument ?
				if (args.length > 0) {
					k = Integer.parseInt(args[0]);

					// On contraint k à valoir au moins 1
					if (k <= 0) {
						LOG.warning("k must be at least 1, " + k + " given");
						k = 1;
					}
				}
			} catch (NumberFormatException e) {
				LOG.severe("Error for the k argument: " + e.getMessage());
				System.exit(1);
			}

		}
		Configuration conf = new Configuration();
		conf.setInt("k", k);


        Job topProfitJob = new Job(conf, "count_k_profit");
        topProfitJob.setSortComparatorClass(LongWritableInverseComparator.class);

        topProfitJob.setOutputKeyClass(LongWritable.class);
        topProfitJob.setOutputValueClass(Text.class);

        topProfitJob.setMapperClass(ProfitMap.class);
        topProfitJob.setReducerClass(ProfitReduce.class);

        topProfitJob.setInputFormatClass(TextInputFormat.class);
        topProfitJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(topProfitJob, new Path(INPUT_PATH));
        Path outPath = new Path(OUTPUT_PATH + "topProfit-" + Instant.now().getEpochSecond());
        FileOutputFormat.setOutputPath(topProfitJob, outPath);
        topProfitJob.waitForCompletion(true);

        /////////////////////////
        Job sortJob = new Job(conf, "sort_k_profit");
        sortJob.setSortComparatorClass(LongWritableInverseComparator.class);

        sortJob.setOutputKeyClass(LongWritable.class);
        sortJob.setOutputValueClass(Text.class);

        sortJob.setMapperClass(SortMap.class);
        sortJob.setReducerClass(SortReduce.class);

        sortJob.setInputFormatClass(TextInputFormat.class);
        sortJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(sortJob, outPath);
        FileOutputFormat.setOutputPath(sortJob, new Path(OUTPUT_PATH + "topProfit-" + Instant.now().getEpochSecond()));
        sortJob.waitForCompletion(true);



	}
}