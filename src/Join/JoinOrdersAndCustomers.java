package Join;


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
import java.util.ArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;



public class JoinOrdersAndCustomers {

	private static final String INPUT_PATH = "input-join/";
	private static final String OUTPUT_PATH = "output/join-";
	private static final Logger LOG = Logger.getLogger(JoinOrdersAndCustomers.class.getName());

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
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
	
			final int CUSTOMER_LENGTH = 8;
			final int ORDER_LENGTH = 9;
			
			String customerId = "";
			String type = "";
			String content = "";
			
			String[] tuples = values.toString().split("\\|");
			
			if(tuples.length == CUSTOMER_LENGTH){
				customerId = tuples[0];
				type = "CUSTOMER";
				content = tuples[1];
			}
			else if(tuples.length == ORDER_LENGTH){
				customerId = tuples[1];
				type = "ORDER";
				content = tuples[8];
			}
			
			if(!customerId.equals("")){
				context.write(new Text(customerId), new Text( type + "|" + content ));
			}
			
			
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
					
			ArrayList<Text> orders = new ArrayList<>();
			Text customer = new Text();
			
			for(Text t : values){
				
				String type;
				
				if(t.getLength() != 0){
				
					String[] t_tabs = t.toString().split("\\|");
					type = t_tabs[0];
					
					if(type.equals("CUSTOMER")){orders.add(new Text(t_tabs[1]));
						customer = new Text(t_tabs[1]);
					}
					else if(type.equals("ORDER")){
						orders.add(new Text(t_tabs[1]));
					}				
				}
			}
			if(!customer.toString().equals("")){
				for(Text order: orders){
					context.write(customer, order);
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Join");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputValueClass(Text.class); 

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + "-" + MethodHandles.lookup().lookupClass().getSimpleName() + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}
