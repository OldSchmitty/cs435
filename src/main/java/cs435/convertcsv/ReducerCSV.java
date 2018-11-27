package cs435.convertcsv;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerCSV extends Reducer<Text, Text, Text, NullWritable> {


  private NullWritable nullValue;

  public void setup(Context context) throws InterruptedException, IOException {
    // Multiple output setup
    String header = context.getConfiguration().get("CSV Header");
    context.write(new Text(header), nullValue);
  }


  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    for (Text entry: values) {

      context.write(entry, nullValue);
    }
  }

}
