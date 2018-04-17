package job4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job4Reducer1 extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text();
    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {


        Integer counter = 0;
        Integer totalDelay = 0;
        for (Text val : values) {
            totalDelay += Integer.parseInt(val.toString().split("\t")[0]);
            counter++;
        }
        Float avgDelay = totalDelay/(float)counter;
        result.set(Integer.toString(counter)+"\t"+Integer.toString(totalDelay)+"\t"+Float.toString(avgDelay));
        context.write(key, result);

    }
}
