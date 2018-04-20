package job7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job7Mapper2 extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        //String key = value.toString().split("")

        context.write(new Text("SINGLE_KEY"), value);
    }
}