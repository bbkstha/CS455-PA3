package job5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job5Reducer1 extends Reducer<IntWritable,Text,IntWritable,Text> {

    private Text result = new Text();
    @Override
    public void reduce(IntWritable key, Iterable<Text> values,
                   Context context
        ) throws IOException, InterruptedException {


        Integer counter = 0;
        Integer totalDelay = 0;
        for (Text val : values) {
            totalDelay += Integer.parseInt(val.toString().split("\t")[2]);
            counter++;
        }
        result.set(Integer.toString(counter)+"\t"+Integer.toString(totalDelay));
        context.write(key, result);

    }
}
