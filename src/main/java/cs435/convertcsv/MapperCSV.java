package cs435.convertcsv;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperCSV  extends Mapper< Object, Text, Text, Text> {

  private final Text outputKey =  new Text();
  private final Text outputValue = new Text();

  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

    String info =  value.toString();

    String removeTail = info.substring(0, info.length() - 2);

    String inputs = removeTail.split("VALUES \\(")[1];
    String[] input  = inputs.split("\\),\\(");

    for (String entry: input) {

      String primaryKey = entry.split(",")[0];

      outputKey.set(primaryKey);
      outputValue.set(entry);

      context.write(outputKey, outputValue);

    }

  }


}
