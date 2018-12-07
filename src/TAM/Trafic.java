package TAM;

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
import java.util.List;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Trafic {
	private static final String INPUT_PATH = "input-TAM/";
	private static final String OUTPUT_PATH = "output/TAM_horairesService-";
	private static final Logger LOG = Logger.getLogger(Trafic.class.getName());

	private static final int STOP_NAME_INDEX = 3; // ex. 'OCCITANIE'
	private static final int ROUTE_NAME_INDEX = 4; // ex. '1' pour Ligne 1
	private static final int DIRECTION_INDEX = 6; // format hh:ii:ss
	private static final int DEPARTURE_TIME_INDEX = 7; // format hh:ii:ss


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
			String hour = tokens[DEPARTURE_TIME_INDEX].split(":")[0]; // sur 'hh:ii:ss' ne garder que 'hh'
			String direction = tokens[DIRECTION_INDEX].equals("1") ? "aller" : "retour";

			Text k = new Text( stop_name + ";" + hour);
			Text v = new Text( direction + ";" + route_name );
			context.write(k, v);

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {


		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int tramsAller = 0, tramsRetour = 0, busAller = 0, busRetour = 0;
			int maxTrams = 0, maxBuses = 0;

			// key : nom de la station + ; + heure
			String[] tokens = key.toString().split(";");
			String stop_name = tokens[0];
			String hour = tokens[1];

			String[] valueTokens;
			String direction;
			int route_name;
			for (Text ligne : values){
				valueTokens = ligne.toString().split(";");
				direction =  valueTokens[0];
				route_name = Integer.parseInt(valueTokens[1]);

				if (direction.equals("aller")){
					if (route_name <= 4){
						tramsAller++;
					}else busAller++;
				}else {
					if (route_name <= 4){
						tramsRetour++;
					}else busRetour++;
				}

			}

			String k = stop_name + "("+hour+"h)";
			String v = "";

			maxTrams = Math.max(tramsAller, tramsRetour);
			maxBuses = Math.max(busAller, busRetour);

			if (maxTrams > 0) {
				v += "trams=";
				if (maxTrams <= 4 ){
					v += "faible";
				}else if (maxTrams <= 9){
					v += "moyen";
				}else v += "fort";
			}

			if (maxTrams > 0 && maxBuses > 0){
				v += "\t";
			}

			if (maxBuses > 0) {
				v += "bus=";
				if (maxBuses <= 4 ){
					v += "faible";
				}else if (maxBuses <= 9){
					v += "moyen";
				}else v += "fort";
			}

			context.write(new Text(k), new Text(v));

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