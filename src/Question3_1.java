
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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.base.CharMatcher;

public class Question3_1 {
	public static class MyMapper1 extends Mapper<Text, Text, Text, StringAndInt> {
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

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

	public static class MyCombiner1 extends Reducer<Text, StringAndInt, Text, StringAndInt> {
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

	public static class MyReducer1 extends Reducer<Text, StringAndInt, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context)
				throws IOException, InterruptedException {

			// Writings tags per country
			for (StringAndInt tagOccurence : values) {
				if (CharMatcher.ASCII.matchesAllOf(tagOccurence.getTag()))
					context.write(new Text(key + " ," + tagOccurence.getTag() + "  , "),
							new IntWritable(tagOccurence.getOccurence()));

			}

		}
	}

	public static class MyMapper2 extends Mapper<Text, Text, Text, StringAndInt> {
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			// First splitting line by line
			for (String line : value.toString().split("\\n")) {
				System.out.println(line);
				String words[] = line.split(","); // Then splitting the line by comma
				context.write(new Text(words[0]), new StringAndInt(words[1], Integer.valueOf(words[2])));
			}
		}
	}

	public static class MyCombiner2 extends Reducer<Text, StringAndInt, Text, StringAndInt> {
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context)
				throws IOException, InterruptedException {

			// Sending to Reducer
			for (StringAndInt tagOccurence : values) {
				context.write(key, tagOccurence);
			}

		}
	}

	public static class MyReducer2 extends Reducer<Text, StringAndInt, Text, IntWritable> {
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

				context.write(new Text(key + " ," + tagOccurence.getTag() + ", "),
						new IntWritable(tagOccurence.getOccurence()));
			}

		}
	}

	public static void main(String[] args) throws Exception {

		// ========================== JOB 1 ==========================================

		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];

		Job job1 = Job.getInstance(conf1, "Question3_1");
		job1.setJarByClass(Question3_1.class);

		job1.setMapperClass(MyMapper1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(StringAndInt.class);

		// Adding combiner
		job1.setCombinerClass(MyCombiner1.class);

		job1.setReducerClass(MyReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		
		FileInputFormat.addInputPath(job1, new Path(input));
		job1.setInputFormatClass(KeyValueTextInputFormat.class);

		FileOutputFormat.setOutputPath(job1, new Path("intermediaire"));
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Waiting for the first job to finish
		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}

		System.out.println("job 1 finished");
		// ========================== JOB 2 ==========================================
		
        //Reading the binary file
		Configuration conf = new Configuration();
        try {

            Path seqFilePath = new Path("intermediaire/part-r-00000");

            SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(seqFilePath));

            Text key = new Text();
            IntWritable val = new IntWritable();

            while (reader.next(key, val)) {
                System.out.println("Sequence File Data: Key: " + key + "\tValue: " + val);
            }

            reader.close();
        } catch(IOException e) {
           
        }

		Configuration conf2 = new Configuration();
		String K = otherArgs[2];

		// Setting configuration to pass K throw context
		conf2.set("K", K);

		Job job2 = Job.getInstance(conf2, "Question3_1");

		job2.setJarByClass(Question3_1.class);

		job2.setMapperClass(MyMapper2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(StringAndInt.class);

		// Adding combiner
		job2.setCombinerClass(MyCombiner2.class);

		job2.setReducerClass(MyReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job2, new Path("intermediaire"));
		job2.setInputFormatClass(SequenceFileInputFormat.class);

		FileOutputFormat.setOutputPath(job2, new Path(output));
		job2.setOutputFormatClass(TextOutputFormat.class);

		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}