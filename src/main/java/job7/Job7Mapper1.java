package job7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job7Mapper1 extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String eachLine = value.toString();
        if(!eachLine.startsWith("Year") && !eachLine.startsWith("NA")){
            String [] elements= eachLine.split(",");
            String originAirport = elements[16].equals("NA") ? "0": elements[16];
            String destAirport = elements[17].equals("NA") ? "0": elements[17];
            String arrDelay = elements[14].equals("NA") ? "0": elements[14];
            String depDelay = elements[15].equals("NA") ? "0": elements[15];
            Integer totalDelay = Integer.parseInt(arrDelay)+Integer.parseInt(depDelay);
            if(elements[23].equals("1")){
                context.write(new Text(originAirport+"\t"+elements[8]+"\t"+elements[10].toString()), new Text(totalDelay.toString()+"\t"+"1"));
                context.write(new Text(destAirport+"\t"+elements[8]+"\t"+elements[10].toString()), new Text(totalDelay.toString()+"\t"+"1"));
            }

        }
    }
}