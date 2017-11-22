
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
import org.codehaus.jackson.map.ser.SerializerCache.TypeKey;

public class Question2_1 {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			for (String line : value.toString().split("\\n")) {
				String word[] = line.split("\\t");
				if (!word[8].isEmpty() && !word[10].isEmpty() && !word[11].isEmpty()) { // Not all fields may have a
																						// value
					String tags[] = word[8].split(",");
					for (String tag : tags) {
						double latitude = Double.valueOf(word[11]);
						double longitude = Double.valueOf(word[10]);
						Country country = Country.getCountryAt(latitude, longitude);
						if (country != null) {
							context.write(new Text(country.toString()), new Text(tag));
						}
					}
				}
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Map<String, Integer> map = new HashMap<String, Integer>();

			for (Text value : values) {
				String val = value.toString();
				if (val.contains("%")) // coded
					val = URLDecoder.decode(value.toString(), "UTF-8"); // decode
				if (map.containsKey(val)) {
					map.put(val, map.get(val) + 1);
				} else {
					map.put(val, 1);
				}
			}

			for (Map.Entry<String, Integer> entry : map.entrySet()) {
				context.write(new Text(entry.getKey() + " - " + key), new Text(entry.getValue().toString()));
			}

			/*
			 * for (Text value : values) { context.write(key, new Text(value)); }
			 */

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];

		Job job = Job.getInstance(conf, "Question2_1");
		job.setJarByClass(Question2_1.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}