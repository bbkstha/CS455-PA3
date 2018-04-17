package job4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Job4Mapper1 extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String eachLine = value.toString();
        if(!eachLine.startsWith("Year") && !eachLine.startsWith("NA")) {
            String[] elements = eachLine.split(",");
            if (!elements[8].contains("NA")) {
                String carrierCode = elements[8];
                String arrDelay = elements[14].equals("NA") ? "0" : elements[14];
                String depDelay = elements[15].equals("NA") ? "0" : elements[15];
                Integer totalDelay = Integer.parseInt(arrDelay) + Integer.parseInt(depDelay);
                context.write(new Text(carrierCode), new Text(Integer.toString(totalDelay)));
            }
        }
    }
}
