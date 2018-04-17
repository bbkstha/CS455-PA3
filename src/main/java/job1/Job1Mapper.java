package job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//Job1 Mapper
public class Job1Mapper
        extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String eachLine = value.toString();
        if(!eachLine.startsWith("Year") && !eachLine.startsWith("NA")) {
            String[] elements = eachLine.split(",");

            if (!elements[5].contains("NA")) {
                Integer depTimeHour = Integer.parseInt(elements[5])/100;
                Integer dayOfWeek = Integer.parseInt(elements[3]);
                Integer monthOfYear = Integer.parseInt(elements[1]);

                String arrDelay = elements[14].equals("NA") ? "0" : elements[14];
                String depDelay = elements[15].equals("NA") ? "0" : elements[15];
                Integer totalDelay = Integer.parseInt(arrDelay) + Integer.parseInt(depDelay);

                context.write(new Text(Integer.toString(depTimeHour)), new Text("H"+Integer.toString(totalDelay)));
                context.write(new Text(Integer.toString(dayOfWeek)), new Text("W"+Integer.toString(totalDelay)));
                context.write(new Text(Integer.toString(monthOfYear)), new Text("M"+Integer.toString(totalDelay)));
            }
        }
    }
}