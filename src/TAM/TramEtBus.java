package TAM;

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
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class TramEtBus {
	private static final String INPUT_PATH = "input-TAM/";
	private static final String OUTPUT_PATH = "output/TAM_horairesService-";
	private static final Logger LOG = Logger.getLogger(TramEtBus.class.getName());



	private static final int STOP_NAME_INDEX = 3; // ex. 'OCCITANIE'
	private static final int ROUTE_NAME_INDEX = 4; // ex. '1' pour Ligne 1


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

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (key.get() == 0) return; // passer le header CSV

			String[] tokens = value.toString().split(";");
			String stop_name  = tokens[STOP_NAME_INDEX];
			String route_name  = tokens[ROUTE_NAME_INDEX];


			Text k = new Text( stop_name);
			context.write(k, new Text(route_name));

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


			int trams = 0;
			int buses = 0;
			for(Text t: values){
				int route_name = Integer.parseInt(t.toString());
				if (route_name <= 4){
					trams++;
				}else {
					buses++;
				}
			}

			context.write(key, new Text(buses + "\t" + trams));

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "ServicesHoraire");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + "-" + MethodHandles.lookup().lookupClass().getSimpleName() + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}