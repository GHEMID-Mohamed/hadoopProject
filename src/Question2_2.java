
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

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
import org.apache.hadoop.util.GenericOptionsParser;

public class Question2_2 {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// First splitting line by line
			for (String line : value.toString().split("\\n")) {
				String word[] = line.split("\\t"); // Then splitting the line by tabulation
				if (!word[8].isEmpty() && !word[10].isEmpty() && !word[11].isEmpty()) { // Not all fields may have a
																						// value
					String tags[] = word[8].split(","); // Getting tags and splitting them by comma
					for (String tag : tags) {
						double latitude = Double.valueOf(word[11]); // getting latitude
						double longitude = Double.valueOf(word[10]); // getting longitude
						Country country = Country.getCountryAt(latitude, longitude); // getting country
						if (country != null) {
							context.write(new Text(country.toString()), new StringAndInt(tag, 1)); // Mapping country by
																									// tag
						}
					}
				}
			}
		}
	}

	public static class MyCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt> {
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context)
				throws IOException, InterruptedException {

			Map<String, Integer> map = new HashMap<String, Integer>();

			// Converting values to hashMap
			for (StringAndInt value : values) {
				String val = value.getTag();
				if (val.contains("%")) // coded
					val = URLDecoder.decode(value.getTag(), "UTF-8"); // decode
				if (map.containsKey(val)) {
					map.put(val, map.get(val) + 1); // Increment value when it's the same key
				} else {
					map.put(val, 1); // Initial value
				}
			}

			// Convert the hashMap to a list in order to sort it
			List<StringAndInt> list = new ArrayList<StringAndInt>();

			for (Map.Entry<String, Integer> entry : map.entrySet()) {
				list.add(new StringAndInt(entry.getKey(), Integer.valueOf(entry.getValue().toString())));
			}

			// Sort the list decremently using compareTo in StringAndInt
			Collections.sort(list);

			// Sending sorted list to Reducer
			for (StringAndInt tagOccurence : list) {
				context.write(key, tagOccurence);
			}

		}
	}

	public static class MyReducer extends Reducer<Text, StringAndInt, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context)
				throws IOException, InterruptedException {

			PriorityQueue<StringAndInt> queue = new PriorityQueue<StringAndInt>();

			// Getting sorted list from Combiner
			for (StringAndInt tagOccurence : values) {
				queue.add(new StringAndInt(tagOccurence.getTag(), Integer.valueOf(tagOccurence.getOccurence())));
			}

			// Getting K passing in parameters
			int k = Integer.valueOf(context.getConfiguration().get("K"));

			int kElements = queue.size() - k;

			// Writing K tags most used
			while (queue.size() > kElements && queue.size() != 0) {

				StringAndInt tagOccurence = queue.remove();

				context.write(new Text("country : " + key + " | tag : " + tagOccurence.getTag() + " | occurences  :"),
						new IntWritable(tagOccurence.getOccurence()));
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		String K = otherArgs[2];

		// Setting configuration to pass K throw context
		conf.set("K", K);

		Job job = Job.getInstance(conf, "Question2_2");
		job.setJarByClass(Question2_2.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndInt.class);

		// Adding combiner
		job.setCombinerClass(MyCombiner.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}