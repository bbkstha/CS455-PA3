package job6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;

public class Job6Reducer1 extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text();
    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        int counter = 0;
        int totalWeatherDelay = 0;
        for (Text val : values) {
            totalWeatherDelay += Integer.parseInt(val.toString());
            counter++;
        }
        Float avgWeatherDelay = totalWeatherDelay/(float)counter;
//        System.out.println("The total number is: "+counter);
//        System.out.println("The total is: "+totalWeatherDelay);
//        System.out.println("The avg delay is: "+avgWeatherDelay);
        result.set(Integer.toString(counter)+"\t"+Integer.toString(totalWeatherDelay)+"\t"+Float.toString(avgWeatherDelay));
        context.write(key, result);
    }
}

