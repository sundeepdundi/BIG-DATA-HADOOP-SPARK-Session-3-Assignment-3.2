package mapreduce.television;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterRecords {
	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();

		Job mapOnlyJob = new Job(c, "FilterRecords");
		mapOnlyJob.setJarByClass(FilterRecords.class);
		mapOnlyJob.setMapperClass(MapForFilterRecords.class);

		mapOnlyJob.setOutputKeyClass(Text.class);
		mapOnlyJob.setOutputValueClass(Text.class);
		mapOnlyJob.setNumReduceTasks(0);
		FileInputFormat.addInputPath(mapOnlyJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(mapOnlyJob, new Path(args[1]));

		System.exit(mapOnlyJob.waitForCompletion(true) ? 0 : 1);
	}
}
