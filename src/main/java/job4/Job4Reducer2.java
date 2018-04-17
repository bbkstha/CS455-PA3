package job4;

import job1.CustomUtility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Job4Reducer2 extends Reducer<Text,Text,Text,Text> {

    private HashMap<String, Integer> carrierToCountDelayMap = new HashMap<>();
    private HashMap<String, Integer> carrierToSumDelayMap = new HashMap<>();
    private HashMap<String, Float> carrierToAvgDelayMap = new HashMap<>();
    private HashMap<String, String> carrierLookupTable = new HashMap<String, String>();

    public void setup(Context context) throws IOException {

        Configuration conf = context.getConfiguration();
        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
        if (cacheFiles != null && cacheFiles.length > 0) {
            for (Path p : cacheFiles) {
                FileReader file = new FileReader(p.toString());
                BufferedReader br = new BufferedReader(file);
                String str0 = null;
                while ((str0 = br.readLine())!=null) {
                    //String str = br.readLine();
                    //if (str == null) break;
//                    if (str0.startsWith("\"iata\"") && str0.split(",").length == 7) {//System.out.println("get1");
//                        String str = null;
//                        while((str = br.readLine())!=null){
//                            String[] split = str.split(",");
//                            String airportID = split[0].replaceAll("\"", "");
//                            String airportName = split[1].replaceAll("\"", "");
//                            String cityName = split[2].replaceAll("\"", "");
//                            String stateName = split[3].replaceAll("\"", "");
//                            airportLookupTable.put(airportID, (airportName + "," + cityName + "," + stateName));
//                        }
//                    }else if (str0.startsWith("tailnum") && str0.split(",").length == 9) {//System.out.println("get1");
//                        String str = null;
//                        while ((str = br.readLine()) != null) {
//                            String[] split = str.split(",");
//                            if (split.length == 9) {
//                                String tailNumber = split[0];
//                                //System.out.println("The year is: "+split[8]);
//                                if((!split[8].contains("year")&&!split[8].contains("None"))) {
//                                    Integer builtYear = Integer.parseInt(split[8]);
//                                    Integer planeAge = 2009 - builtYear; //newest airplane can be built by 2008
//                                    String ageTag = (planeAge < 20) ? "NEW" : "OLD";
//                                    planeLookupTable.put(tailNumber, (planeAge.toString() + "\t" + ageTag));
//                                }
//                                //System.out.println("The palne lookup buiding..key is: " + tailNumber);
//                                //System.out.println("The plane lookup buiding..value is: " + ageTag);
//                            }
//                        }
//                    }
                    if (str0.startsWith("Code") && str0.split(",").length == 2) {//System.out.println("get1");
                        String str = null;
                        while((str = br.readLine())!=null){
                            String[] split = str.split(",");
                            String carriedCode = split[0].replaceAll("\"", "");
                            String carrierName = split[1].replaceAll("\"", "");
                            carrierLookupTable.put(carriedCode, carrierName);
                        }
                    }
                }
            }
        }
    }

    public void reduce (Text key, Iterable < Text > values,
                        Context context
    ) throws IOException, InterruptedException {

        CustomUtility util = new CustomUtility();

        for (Text result : values) {
            String[] elements = result.toString().split("\t");
            String carrierCode = elements[0];
            Integer numberOfCarrierDelay = Integer.parseInt(elements[1]);
            Integer totalDelay = Integer.parseInt(elements[2]);
            Float avgDelay = Float.parseFloat(elements[3]);

            carrierToCountDelayMap.put(carrierCode, numberOfCarrierDelay);
            carrierToSumDelayMap.put(carrierCode, totalDelay);
            carrierToAvgDelayMap.put(carrierCode, avgDelay);
        }
        Map<String, Integer> sortedCountDelay = util.sortByComparatorInteger(carrierToCountDelayMap, false);
        Map<String, Integer> sortedTotalDelay = util.sortByComparatorInteger(carrierToSumDelayMap, false);
        Map<String, Float> sortedAvgDelay = util.sortByComparatorFloat(carrierToAvgDelayMap, false);


        context.write(new Text("Summary of Question 4"), null);
        context.write(null, new Text("\n"));
        context.write(null, new Text("Top 10 carriers with most number of delays:"));
        Set<String> keys = sortedCountDelay.keySet();
        String[] keysArray = keys.toArray(new String[keys.size()]);
        for (int i = 0; i < keysArray.length && i < 10; i++) {
            context.write(new Text(Integer.toString(i + 1) + ". " + carrierLookupTable.get(keysArray[i])), new Text("\t\t\tNumber of times delays occured: "+Integer.toString(sortedCountDelay.get(keysArray[i]))));
        }
        //System.out.println("The value of lookup is: "+airportLookupTable.get("ORD"));
        context.write(null, new Text("\n"));
        context.write(null, new Text("Top 10 Carriers with highest sum total delays:"));
        Set<String> keys1 = sortedTotalDelay.keySet();
        String[] keysArray1 = keys1.toArray(new String[keys1.size()]);
        for (int i = 0; i < keysArray1.length && i < 10; i++) {
            //System.out.println("The keys are: "+keysArray1[i]);
            //System.out.println("The value of lookup inside is: "+airportLookupTable.get(keysArray1[i]));
            context.write(new Text(Integer.toString(i + 1) + ". " + carrierLookupTable.get(keysArray1[i])), new Text("\t\t\tTotal delays in minutes: "+Integer.toString(sortedTotalDelay.get(keysArray1[i]))));
        }

        context.write(null, new Text("\n"));
        context.write(null, new Text("Top 10 carriers with highest average delays:"));
        Set<String> keys2 = sortedAvgDelay.keySet();
        String[] keysArray2 = keys2.toArray(new String[keys2.size()]);
        for (int i = 0; i < keysArray2.length && i < 10; i++) {
            context.write(new Text(Integer.toString(i + 1) + ". " + carrierLookupTable.get(keysArray2[i])), new Text("\t\t\tAvegrage Delay in minutes: "+Float.toString(sortedAvgDelay.get(keysArray2[i]))));
        }
    }
}
