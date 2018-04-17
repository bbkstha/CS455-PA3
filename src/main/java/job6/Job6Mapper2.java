package job6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Job6Mapper2 extends Mapper<Object, Text, Text, Text> {

   // private HashMap<String, String> airportLookupTable = new HashMap<String, String>();

//    public void setup(Context context) throws IOException {
//
//        Configuration conf = context.getConfiguration();
//        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
//        if (cacheFiles != null && cacheFiles.length > 0)
//        {
//            FileReader file = new FileReader(cacheFiles[0].toString());
//            BufferedReader br = new BufferedReader(file);
//            while (true) {
//                String str = br.readLine();
//                if(str==null) break;
//                else if (str.split(",").length==7) {//System.out.println("get1");
//                    String[] split = str.split(",");
//                    //
//                    String airportID = split[0].replaceAll("\"","");
//                    String airportName = split[1];
//                    String cityName = split[2];
//                    String stateName = split[3];
//                    airportLookupTable.put(airportID, (airportName+"\t"+cityName+"\t"+stateName));
//                    System.out.println("The lookup buiding..key is: "+airportID);
//                    //System.out.println("The lookup buiding..value is: "+airportLookupTable.get(airportID));
//                }
//            }
//        }
//    }

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