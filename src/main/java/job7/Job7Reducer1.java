package job7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Job7Reducer1 extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text();
    HashMap<Integer, Text> tmpCopy = new HashMap<>();
    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        int counter = 0;
        int totalDelay = 0;
        for (Text val : values) {
            totalDelay += Integer.parseInt(val.toString().split("\t")[0]);
            counter+=Integer.parseInt(val.toString().split("\t")[1]);
            //tmpCopy.put(counter, val);
        }
       //Float avgDelay = totalDelay/(float)counter;;
        //for(Text v: tmpCopy.values()) {
            result.set(Integer.toString(counter) + "\t" + Integer.toString(totalDelay));
            context.write(key, result);
        //}
    }
}

