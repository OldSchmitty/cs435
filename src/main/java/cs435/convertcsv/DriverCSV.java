package cs435.convertcsv;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver for class that converted SQL insert statements into a CSV file
 */
public class DriverCSV {

  /**
   * Creates a CSV file from a SQL insert statement
   * @param args [0] file input dir, [1] output file dir, [3] header for CSV, [4] number of reducers
   * @throws IOException problem reading file
   * @throws ClassNotFoundException
   * @throws InterruptedException program interrupted
   */
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {

    // Setup job
    Configuration conf = new Configuration();
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);

    Job job = Job.getInstance(conf, "Convert to CSV: " + input.getName());
    job.getConfiguration().set("mapreduce.output.basename", input.getName());

    job.getConfiguration().set("CSV Header", args[2]);

    Integer numReducers = Integer.parseInt(args[3]);

    String header = job.getConfiguration().get("CSV Header");

    System.out.println("Header: " + header);
    System.out.println("Input: " + input.getName());
    System.out.println("Output: " + output.getName());

    // Setup Mapper
    job.setInputFormatClass(TextInputFormat.class);
    job.setJarByClass(DriverCSV.class);
    job.setMapperClass(MapperCSV.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(ReducerCSV.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(numReducers);

    // Setup path arguments
    FileInputFormat.addInputPath(job, input);

    // Remove output path if it exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(output)) {
      hdfs.delete(output, true);
    }

    FileOutputFormat.setOutputPath(job, output);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
