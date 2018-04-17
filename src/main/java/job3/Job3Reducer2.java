package job3;

import job1.CustomUtility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.hash.Hash;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Job3Reducer2 extends Reducer<Text,Text,Text,Text> {

    private HashMap<String, Integer> airportTrafficeMap = new HashMap<>();
    private HashMap<Integer, HashMap<String, Integer>> airportTrafficeMapPerYear = new HashMap<>();
    private HashMap<String, String> airportLookupTable = new HashMap<String, String>();

    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
        if (cacheFiles != null && cacheFiles.length > 0)

        {
            for (Path p : cacheFiles) {
                FileReader file = new FileReader(p.toString());
                BufferedReader br = new BufferedReader(file);
                String str0 = null;
                while ((str0 = br.readLine()) != null) {
                    //String str = br.readLine();
                    //if (str == null) break;
                    if (str0.startsWith("\"iata\"") && str0.split(",").length == 7) {//System.out.println("get1");
                        String str = null;
                        while ((str = br.readLine()) != null) {
                            String[] split = str.split(",");
                            //
                            String airportID = split[0].replaceAll("\"", "");
                            String airportName = split[1].replaceAll("\"", "");
                            String cityName = split[2].replaceAll("\"", "");
                            String stateName = split[3].replaceAll("\"", "");
                            airportLookupTable.put(airportID, (airportName + "," + cityName + "," + stateName));
                            //System.out.println("The lookup buiding..key is: "+airportID);
                            // System.out.println("The lookup buiding..value is: " + airportLookupTable.get(airportID));
                        }
                    }
                }
            }
        }
    }


    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {

        CustomUtility util = new CustomUtility();
        for (Text result : values) {
            String[] elements = result.toString().split("\t");
            if (elements.length == 2) {
                String airportID = elements[0];
                Integer numberOfFlightsInOut = Integer.parseInt(elements[1]);
                airportTrafficeMap.put(airportID, numberOfFlightsInOut);
            } else if (elements.length == 3) {
                Integer year = Integer.parseInt(elements[0]);
                String airportID = elements[1];
                Integer numberOfFlightsInOut = Integer.parseInt(elements[2]);
                HashMap<String, Integer> tmpHashMap = new HashMap<>();
                tmpHashMap.put(airportID, numberOfFlightsInOut);
                airportTrafficeMapPerYear.put(year, tmpHashMap);
            }
        }

        Map<String, Integer> sortedCountTraffice = util.sortByComparatorInteger(airportTrafficeMap, false);
        context.write(new Text("Summary of Question 3"), null);
        context.write(null, new Text("\n"));
        context.write(null, new Text("Top 10 Busiest Airports:"));
        Set<String> keys = sortedCountTraffice.keySet();
        String[] keysArray = keys.toArray(new String[keys.size()]);
        for (int i = 0; i < keysArray.length && i < 10; i++) {
            context.write(new Text(Integer.toString(i + 1) + ". " + airportLookupTable.get(keysArray[i])), new Text("\t\t\tNumber of Traffic in whole dataset: "+Integer.toString(sortedCountTraffice.get(keysArray[i]))));
        }


        for (Map.Entry<Integer, HashMap<String, Integer>> entrySet : airportTrafficeMapPerYear.entrySet()) {
            int eachYear = entrySet.getKey();
            HashMap<String, Integer> airportMap = entrySet.getValue();
            List<String> sortedyairports = new LinkedList<>();
            context.write(null, new Text("\n"));
            context.write(null, new Text("Top Busiest Airports Per Year:"));
            context.write(null, new Text("YEAR: "+eachYear));
            Map<String, Integer> sortedCountTrafficePerYear = util.sortByComparatorInteger(airportMap, false);
            Set<String> keys1 = sortedCountTrafficePerYear.keySet();
            String[] keysArray1 = keys1.toArray(new String[keys.size()]);
            System.out.println("The keyArray length is: "+keysArray1.length);
            for (int i = 0; i < keysArray1.length && i<10; i++) {
                if (keysArray1[i] != null) {
                    //System.out.println("The keyArray content is: " + keysArray1[i]);
                    //System.out.println("The lookup result: " + airportLookupTable.get(keysArray1[i]) +
                    //        " and the value is: " + Integer.toString(sortedCountTrafficePerYear.get(keysArray1[i])));

                    context.write(new Text(Integer.toString(i + 1) + ". " + airportLookupTable.get(keysArray1[i])), new Text("\t\t\tNumber of Traffic in Per Year: "+Integer.toString(sortedCountTrafficePerYear.get(keysArray1[i]))));
                }
            }
        }
    }
}

