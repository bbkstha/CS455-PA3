package job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Job1Reducer
            extends Reducer<Text,Text,Text,Text> {

        private Text result = new Text();
        private HashMap<String, Integer> hubs = new HashMap<String, Integer>();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int counter = 0;
            int totalDelay = 0;
            Character firstChar = null;
            for (Text val : values) {
                firstChar = val.toString().charAt(0);
                totalDelay += Integer.parseInt(val.toString().substring(1));
                counter++;
            }
            Float avgDelay = totalDelay/(float)counter;
            String output = Integer.toString(counter)+"\t"+Integer.toString(totalDelay)+"\t"+Float.toString(avgDelay);
            if(firstChar.equals('H'))
                context.write(key, new Text("H"+output));
            else if(firstChar.equals('W'))
                context.write(key, new Text("W"+output));
            if(firstChar.equals('M'))
                context.write(key, new Text("M"+output));
        }
    }
