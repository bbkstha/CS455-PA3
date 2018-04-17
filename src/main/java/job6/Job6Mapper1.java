package job6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;

public class Job6Mapper1 extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String eachLine = value.toString();
        if(!eachLine.startsWith("Year") && !eachLine.startsWith("NA")){
            String [] elements= eachLine.split(",");
            String originAirport = elements[16].equals("NA") ? "0": elements[16];
            String destAirport = elements[17].equals("NA") ? "0": elements[17];
            String weatherDelay = elements[25].equals("NA") ? "0": elements[25];
            context.write(new Text(originAirport), new Text(weatherDelay));
            context.write(new Text(destAirport), new Text(weatherDelay));
        }
    }
}
