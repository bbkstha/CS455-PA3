package job3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job3Mapper1 extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String eachLine = value.toString();
        if(!eachLine.startsWith("Year") && !eachLine.startsWith("NA")){
            String [] elements= eachLine.split(",");
            String originAirport = elements[16].equals("NA") ? "0": elements[16];
            String destAirport = elements[17].equals("NA") ? "0": elements[17];
            String year = elements[0].equals("NA") ? "0": elements[0];
            context.write(new Text(originAirport), new Text("1"));
            context.write(new Text(destAirport), new Text("1"));
            context.write(new Text(year+"\t"+originAirport), new Text("1"));
            context.write(new Text(year+"\t"+destAirport), new Text("1"));
        }
    }
}
