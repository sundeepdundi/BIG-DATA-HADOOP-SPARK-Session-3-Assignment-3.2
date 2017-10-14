package mapreduce.television;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapForFilterRecords extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String NA = "NA";
		String[] words = line.split("\\|");
		String companyName = words[0];

		String productName = words[1];
		if (!(companyName.equals(NA) || productName.equals(NA))) {
			context.write(value, null);
		}

	}
}
