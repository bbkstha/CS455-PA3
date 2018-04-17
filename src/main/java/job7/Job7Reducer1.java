package job7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class Job7Reducer1 extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text();
    ArrayList<Text> tmpCopy = new ArrayList<>();
    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        int counter = 0;
        int totalDelay = 0;
        for (Text val : values) {
            tmpCopy.add(val);
            totalDelay += Integer.parseInt(val.toString().split("\t")[0]);
            counter++;
        }
       // Float avgWeatherDelay = totalWeatherDelay/(float)counter;
//        System.out.println("The total number is: "+counter);
//        System.out.println("The total is: "+totalWeatherDelay);
//        System.out.println("The avg delay is: "+avgWeatherDelay);
        for(Text v: tmpCopy) {
            result.set(Integer.toString(counter) + "\t" + Integer.toString(totalDelay)+"\t"+v.toString());
            context.write(key, result);
        }
    }
}

