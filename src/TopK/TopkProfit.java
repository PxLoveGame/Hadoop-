package TopK;

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


/*
 * Jusqu'à présent nous avons défini nos mappers et reducers comme des classes internes à notre classe principale.
 * Dans des applications réelles de map-reduce cela ne sera généralement pas le cas, les classes seront probablement localisées dans d'autres fichiers.
 * Dans cet exemple, nous avons défini TopK.ProfitMap et TopK.ProfitReduce en dehors de notre classe principale.
 * Il se pose alors le problème du passage du paramètre 'k' dans notre reducer, car il n'est en effet plus possible de déclarer un paramètre k dans notre classe principale qui serait partagé avec ses classes internes ; c'est la que la Configuration du Job entre en jeu.
 */


//class LongWritableInverseComparator extends InverseComparator<LongWritable> {
//
//	public LongWritableInverseComparator() {
//		super(LongWritable.class);
//	}
//}

// =========================================================================
// MAPPERS
// =========================================================================

class ProfitMap extends Mapper<LongWritable, Text, LongWritable, Text> {

//	private int k;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() == 0) return; // retirer le header CSV

		String[] values = value.toString().split(",");

		long profit = (long) Float.parseFloat(values[20]);
//		String customerId = values[5];

		context.write(new LongWritable(profit), value);
//		System.out.println("Mapping " + profit + " ==> " + value);

	}
}

// =========================================================================
// REDUCERS
// =========================================================================

class ProfitReduce extends Reducer<LongWritable, Text, LongWritable, Text> {
	private int k;
	private TreeMap<Long, ArrayList<Text>> profits = new TreeMap<>();

	/**
	 * Méthode appelée avant le début de la phase reduce.
	 */
	@Override
	public void setup(Context context) {
		// On charge k
		k = context.getConfiguration().getInt("k", 1);
	}

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) {

		ArrayList<Text> vals = new ArrayList<>();
		for (Text t : values) vals.add(t);

		profits.put(key.get(), vals);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {

		List<Long> firstKeys = new ArrayList<>(profits.keySet());
		Collections.sort(firstKeys);				// sort (low to high)
		Collections.reverse(firstKeys);				// reverse (high to low)
		firstKeys = firstKeys.subList(0, k);		// keep it to a minimum


		int count = 0; // k entries maximum
		for (Long currentKey : firstKeys){
			Iterable<Text> texts = profits.get(currentKey);
			for (Text currentText : texts){
				if (count++ < k){ // write at most k entries
					context.write(new LongWritable(currentKey), currentText);
				}
			}
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


		Job topProfitJob = new Job(conf, "top_k_profit");
//		topProfitJob.setSortComparatorClass(LongWritableInverseComparator.class);
		topProfitJob.setOutputKeyClass(LongWritable.class);
		topProfitJob.setOutputValueClass(Text.class);

		topProfitJob.setMapperClass(ProfitMap.class);
		topProfitJob.setReducerClass(ProfitReduce.class);

		topProfitJob.setInputFormatClass(TextInputFormat.class);
		topProfitJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(topProfitJob, new Path(INPUT_PATH));
		String tempOutputPath = OUTPUT_PATH + "topProfit-" + Instant.now().getEpochSecond();
		FileOutputFormat.setOutputPath(topProfitJob, new Path(tempOutputPath));
		topProfitJob.waitForCompletion(true);



	}
}