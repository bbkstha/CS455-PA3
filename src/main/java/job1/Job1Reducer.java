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


            for (Text val : values) {
                //firstChar = val.toString().charAt(0);
                    totalDelay += Integer.parseInt(val.toString());
                    counter++;
            }
            Character firstChar = key.toString().charAt(0);
            Float avgDelay = totalDelay/(float)counter;
            String output = Integer.toString(counter)+"\t"+Integer.toString(totalDelay)+"\t"+Float.toString(avgDelay);
//            if(firstChar.equals('H'))
//                context.write(new key, new Text(output));
//            else if(firstChar.equals('W'))
//                context.write(new Text("W"+key.toString()), new Text(output));
//            if(firstChar.equals('M'))
//                context.write(new Text("W"+key.toString()), new Text(output));
            context.write(key, new Text(output));

        }
    }
