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
import java.time.Instant;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


/*
 * Jusqu'à présent nous avons défini nos mappers et reducers comme des classes internes à notre classe principale.
 * Dans des applications réelles de map-reduce cela ne sera généralement pas le cas, les classes seront probablement localisées dans d'autres fichiers.
 * Dans cet exemple, nous avons défini ProfitMap et ProfitReduce en dehors de notre classe principale.
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

class SortMap extends Mapper<LongWritable, Text, LongWritable, Text> {

//	private int k;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() == 0) return; // retirer le header CSV

		final int k = context.getConfiguration().getInt("k", 1);

		String[] values = value.toString().split(",");

		long profit = (long) Float.parseFloat(values[20]);
//        int rowid = Integer.parseInt(values[0]);
		String customerId = values[5];

		context.write(new LongWritable(profit), value);

	}
}

class FilterMap extends Mapper<LongWritable, Text, LongWritable, Text> {

//	private int k;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		System.out.println("FilterMap : " + key + " ==> " + value);

	}
}

// =========================================================================
// REDUCERS
// =========================================================================

class SortReduce extends Reducer<LongWritable, Text, LongWritable, Text> {
	/**
	 * ProfitMap avec tri suivant l'ordre naturel de la clé (la clé représentant la fréquence d'un ou plusieurs mots).
	 * Utilisé pour conserver les k mots les plus fréquents.
	 *
	 * Il associe une fréquence à une liste de mots.
	 */
	private TreeMap<Integer, List<Text>> sortedWords = new TreeMap<>();
	private int nbsortedWords = 0;
	private int k;

	/**
	 * Méthode appelée avant le début de la phase reduce.
	 */
	@Override
	public void setup(Context context) {
		// On charge k
		k = context.getConfiguration().getInt("k", 1);
	}

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// Le reducer recoit dans l'ordre (via le shuffle) les commandes par profit décroissant


		for (Text value : values){
			System.out.println("ProfitReduce " + key + " ==> " + value);
			context.write(key, value);
		}
	}

	/**
	 * Méthode appelée à la fin de l'étape de reduce.
	 *
	 * Ici on envoie les mots dans la sortie, triés par ordre descendant.
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		Integer[] nbofs = sortedWords.keySet().toArray(new Integer[0]);

		// Parcours en sens inverse pour obtenir un ordre descendant
		int i = nbofs.length;

		while (i-- != 0) {
			Integer nbof = nbofs[i];

//			for (Text words : sortedWords.get(nbof))
//				context.write(words, new IntWritable(nbof));
		}
	}
}

class FilterReduce extends Reducer<LongWritable, Text, Text, IntWritable> {
	/**
	 * ProfitMap avec tri suivant l'ordre naturel de la clé (la clé représentant la fréquence d'un ou plusieurs mots).
	 * Utilisé pour conserver les k mots les plus fréquents.
	 *
	 * Il associe une fréquence à une liste de mots.
	 */
	private TreeMap<Integer, List<Text>> sortedWords = new TreeMap<>();
	private int nbsortedWords = 0;
	private int k;

	/**
	 * Méthode appelée avant le début de la phase reduce.
	 */
	@Override
	public void setup(Context context) {
		// On charge k
		k = context.getConfiguration().getInt("k", 1);
	}

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// Le reducer recoit dans l'ordre (via le shuffle) les commandes par profit décroissant


		for (Text value : values){
			System.out.println("ProfitReduce " + key + " ==> " + value);
		}
	}

	/**
	 * Méthode appelée à la fin de l'étape de reduce.
	 *
	 * Ici on envoie les mots dans la sortie, triés par ordre descendant.
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		Integer[] nbofs = sortedWords.keySet().toArray(new Integer[0]);

		// Parcours en sens inverse pour obtenir un ordre descendant
		int i = nbofs.length;

		while (i-- != 0) {
			Integer nbof = nbofs[i];

			for (Text words : sortedWords.get(nbof))
				context.write(words, new IntWritable(nbof));
		}
	}
}

public class TopkWordCount {
	private static final String INPUT_PATH = "input-groupBy/";
	private static final String OUTPUT_PATH = "output/Topk-";
	private static final Logger LOG = Logger.getLogger(TopkWordCount.class.getName());

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


		Job profitSortJob = new Job(conf, "profit_sort");
//		profitSortJob.setSortComparatorClass(LongWritableInverseComparator.class);
		profitSortJob.setOutputKeyClass(LongWritable.class);
		profitSortJob.setOutputValueClass(Text.class);

		profitSortJob.setMapperClass(ProfitMap.class);
		profitSortJob.setReducerClass(ProfitReduce.class);

		profitSortJob.setInputFormatClass(TextInputFormat.class);
		profitSortJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(profitSortJob, new Path(INPUT_PATH));
		String tempOutputPath = OUTPUT_PATH + "sort-" + Instant.now().getEpochSecond();
		FileOutputFormat.setOutputPath(profitSortJob, new Path(tempOutputPath));
		profitSortJob.waitForCompletion(true);
		System.out.println("Fin de process profit_sort vers " + tempOutputPath);

		// --------------------------------------------------

		Job topFilter = new Job(conf, "only_top");
		topFilter.setOutputKeyClass(LongWritable.class);
		topFilter.setOutputValueClass(Text.class);

		topFilter.setMapperClass(FilterMap.class);
		topFilter.setReducerClass(FilterReduce.class);

		topFilter.setInputFormatClass(TextInputFormat.class);
		topFilter.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(topFilter, new Path(tempOutputPath));
		FileOutputFormat.setOutputPath(topFilter, new Path(OUTPUT_PATH + "filter-" + Instant.now().getEpochSecond()));
		topFilter.waitForCompletion(true);


	}
}