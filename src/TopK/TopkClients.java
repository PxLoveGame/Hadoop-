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



class ClientProfitMap extends Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		if (key.get() == 0) return; // avoid CSV header

		String[] tokens = value.toString().split(",");

		String customerId = tokens[5];
		Long profit = (long) Float.parseFloat(tokens[20]);

		context.write(new LongWritable(profit), new Text(customerId));

	}
}

class ClientProfitReduce extends Reducer<LongWritable, Text, LongWritable, Text> {

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {


		for (Text value : values){
//			System.out.println("Reduce : " + key + " ==> " + value);
			context.write(key, value);
		}

	}

}


class TopMap extends Mapper<LongWritable, Text, Text, LongWritable> {
	private int k;

	public void setup(Context context) {
		k = context.getConfiguration().getInt("k", 1);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] tokens = value.toString().split("\t");

		Long profit = (long) Float.parseFloat(tokens[0]);
		String customerId = tokens[1];





		if (key.get() < k * value.getLength()) {
			context.write(new Text(customerId), new LongWritable(profit));
		}

	}
}

class TopReduce extends Reducer<Text, LongWritable, Text, LongWritable> {


	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {


		for (LongWritable value : values){
//			System.out.println("Re_reduce : " + key + " ==> " + value);
			context.write(key, value);
		}
	}

}



public class TopkClients{
	private static final String INPUT_PATH = "input-groupBy/";
	private static final String OUTPUT_PATH = "output/Topk-";
	private static final Logger LOG = Logger.getLogger(TopkClients.class.getName());

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

		Job profitSortJob = new Job(conf, "clients_sort");
		profitSortJob.setSortComparatorClass(LongWritableInverseComparator.class);

		profitSortJob.setOutputKeyClass(LongWritable.class);
		profitSortJob.setOutputValueClass(Text.class);


		profitSortJob.setMapperClass(ClientProfitMap.class);
		profitSortJob.setReducerClass(ClientProfitReduce.class);

		profitSortJob.setInputFormatClass(TextInputFormat.class);
		profitSortJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(profitSortJob, new Path(INPUT_PATH));
		String tempOutputPath = OUTPUT_PATH + "sort-" + Instant.now().getEpochSecond();
		FileOutputFormat.setOutputPath(profitSortJob, new Path(tempOutputPath));
		profitSortJob.waitForCompletion(true);

//		// --------------------------------------------------

		Job topFilter = new Job(conf, "only_top");
        topFilter.setSortComparatorClass(LongWritableInverseComparator.class);
		topFilter.setOutputKeyClass(Text.class);
		topFilter.setOutputValueClass(LongWritable.class);

		topFilter.setMapperClass(TopMap.class);
		topFilter.setReducerClass(TopReduce.class);

		topFilter.setInputFormatClass(TextInputFormat.class);
		topFilter.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(topFilter, new Path(tempOutputPath));
		FileOutputFormat.setOutputPath(topFilter, new Path(OUTPUT_PATH + "filter-" + Instant.now().getEpochSecond()));
		topFilter.waitForCompletion(true);


	}
}