package job4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job4Mapper2 extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
//        String totalWeatherDelays = value.toString().split("\t")[2];
//        String avgWeatherDelays = value.toString().split("\t")[3];

//        String originAirport = elements[16].equals("NA") ? "0": elements[16];
//        String destAirport = elements[17].equals("NA") ? "0": elements[17];
//        String weatherDelay = elements[25].equals("NA") ? "0": elements[25];
//        context.write(new Text(originAirport), new Text(weatherDelay));
//        String key2 = "ORD";
//        System.out.println("The size of lookup is MAPPER: "+airportLookupTable.size());
//        System.out.println("The value of lookup is MAPPER: "+airportLookupTable.get(key2));
            context.write(new Text("SINGLE_KEY"), value);
//        context.write(new CompositeKeyWritable("*"+totalWeatherDelays, avgWeatherDelays), new Text("sumBased"+value.toString()));
//        context.write(new CompositeKeyWritable("^"+totalWeatherDelays, avgWeatherDelays), new Text("avgBased"+value.toString()));
        }
}