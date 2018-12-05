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
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


// =========================================================================
// MAPPERS
// =========================================================================

class TopMap extends Mapper<LongWritable, Text, Text, LongWritable> {

	private int k;


    public void setup(Context context) {
        // On charge k
        k = context.getConfiguration().getInt("k", 1);
    }

    @Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		String[] tokens = value.toString().split("\t");

		String customerId = tokens[0];
		Long profit = (long) Float.parseFloat(tokens[1]);


        if (key.get() < k) {
            System.out.println("k : " + k);
            System.out.println("MMMMap : "+ key.get() + "->" + value);
            System.out.println("Mapmap : " + customerId + " ==>" + profit);
            context.write(new Text(customerId), new LongWritable(profit));
        }else System.out.println("key= " + key);


	}
}

class ClientProfitMap extends Mapper<LongWritable, Text, LongWritable, Text> {


	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		if (key.get() == 0) return; // avoid CSV header

		String[] tokens = value.toString().split(",");

		String customerId;
		Long profit;

		try {
			customerId = tokens[5];
			profit = (long) Float.parseFloat(tokens[20]);
		} catch (IndexOutOfBoundsException e){
			System.err.println("Index impossible pour " + value);
			return;
		}


		context.write(new LongWritable(profit), new Text(customerId));

	}
}

// =========================================================================
// REDUCERS
// =========================================================================

class TopReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
	private TreeMap<Long, String> customers = new TreeMap<>();
	private int k;

	/**
	 * Méthode appelée avant le début de la phase reduce.
	 */
	@Override
	public void setup(Context context) {
		// On charge k
		k = context.getConfiguration().getInt("k", 1);
	}

	private void addCustomer(LongWritable value, Text customerId){
		String custId = customerId.toString();
		Long min_value = Long.MAX_VALUE;

		for (Long v : customers.keySet()){ // update the minimum profit found amongst currently listed customers
			if (v < min_value) min_value = v;
		}

		if (customers.size() >= k && value.get() > min_value){ // add to the top customers list
			customers.remove(min_value);	 // make room for a new entry ( max k entries )
		}

		if (value.get() > min_value || customers.size() < k){ // insert the new value ?
			customers.put(value.get(), custId);
		}
	}

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {


		for (LongWritable value : values){
			addCustomer(value, key);
		}
	}

	/**
	 * Méthode appelée à la fin de l'étape de reduce.
	 *
	 * Ici on envoie les mots dans la sortie, triés par ordre descendant.
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		ArrayList<Map.Entry<Long, String>> entrySet = new ArrayList<>(customers.entrySet());
		Collections.reverse(entrySet);
		for (Map.Entry<Long, String> entry : entrySet){
			String customerId = entry.getValue();
			Long profit = entry.getKey();
			context.write(new Text(customerId), new LongWritable(profit));
		}
	}
}

class ClientProfitReduce extends Reducer<LongWritable, Text, Text, LongWritable> {

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {


        for (Text value : values){
            context.write(value, key);
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
		profitSortJob.setOutputKeyClass(LongWritable.class);
		profitSortJob.setOutputValueClass(Text.class);

		profitSortJob.setSortComparatorClass(LongWritableInverseComparator.class);

		profitSortJob.setMapperClass(ClientProfitMap.class);
		profitSortJob.setReducerClass(ClientProfitReduce.class);

		profitSortJob.setInputFormatClass(TextInputFormat.class);
		profitSortJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(profitSortJob, new Path(INPUT_PATH));
		String tempOutputPath = OUTPUT_PATH + "sort-" + Instant.now().getEpochSecond();
		FileOutputFormat.setOutputPath(profitSortJob, new Path(tempOutputPath));
		profitSortJob.waitForCompletion(true);

//		// --------------------------------------------------
//
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