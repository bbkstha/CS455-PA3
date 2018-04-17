package job6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import job1.CustomUtility;

import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;


public class Job6Reducer2 extends Reducer<Text,Text,Text,Text> {

    private HashMap<String, Integer> airportToCountDelayMap = new HashMap<>();
    private HashMap<String, Integer> airportToSumDelayMap = new HashMap<>();
    private HashMap<String, Float> airportToAvgDelayMap = new HashMap<>();
    private HashMap<String, String> airportLookupTable = new HashMap<String, String>();
    private HashMap<String, String> planeLookupTable = new HashMap<String, String>();

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
                    if (str0.startsWith("\"iata\"") && str0.split(",").length == 7) {//System.out.println("get1");
                        String str = null;
                        while((str = br.readLine())!=null){
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
                    }else if (str0.startsWith("tailnum") && str0.split(",").length == 9) {//System.out.println("get1");
                        String str = null;
                        while ((str = br.readLine()) != null) {
                            String[] split = str.split(",");
                            if (split.length == 9) {
                                String tailNumber = split[0];
                                //System.out.println("The year is: "+split[8]);
                                Integer builtYear = (split[8].contains("year")||split[8].contains("None")) ? 0 : Integer.parseInt(split[8]);
                                String ageTag = ((2018 - builtYear) < 20) ? "NEW" : "OLD";
                                planeLookupTable.put(tailNumber, ageTag);
                                System.out.println("The palne lookup buiding..key is: " + tailNumber);
                                System.out.println("The plane lookup buiding..value is: " + ageTag);
                            }
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
                String airportID = elements[0];
                Integer numberOfWeatherDelay = Integer.parseInt(elements[1]);
                Integer totalWeatherDelay = Integer.parseInt(elements[2]);
                Float avgWeatherDelay = Float.parseFloat(elements[3]);

                airportToCountDelayMap.put(airportID, numberOfWeatherDelay);
                airportToSumDelayMap.put(airportID, totalWeatherDelay);
                airportToAvgDelayMap.put(airportID, avgWeatherDelay);
            }
            Map<String, Integer> sortedCountDelay = util.sortByComparatorInteger(airportToCountDelayMap, false);
            Map<String, Integer> sortedTotalDelay = util.sortByComparatorInteger(airportToSumDelayMap, false);
            Map<String, Float> sortedAvgDelay = util.sortByComparatorFloat(airportToAvgDelayMap, false);


            context.write(new Text("Summary of Question 6"), null);
            context.write(null, new Text("\n"));
            context.write(null, new Text("Top 10 Airports/Cities with most number of weather-realted delays:"));
            Set<String> keys = sortedCountDelay.keySet();
            String[] keysArray = keys.toArray(new String[keys.size()]);
            for (int i = 0; i < keysArray.length && i < 10; i++) {
                context.write(new Text(Integer.toString(i + 1) + ". " + airportLookupTable.get(keysArray[i])), new Text("\t\t\tNumber of times delay occured: "+Integer.toString(sortedCountDelay.get(keysArray[i]))));
            }
            //System.out.println("The value of lookup is: "+airportLookupTable.get("ORD"));
            context.write(null, new Text("\n"));
            context.write(null, new Text("Top 10 Airports/Cities with highest sum total weather-realted delays:"));
            Set<String> keys1 = sortedTotalDelay.keySet();
            String[] keysArray1 = keys1.toArray(new String[keys1.size()]);
            for (int i = 0; i < keysArray1.length && i < 10; i++) {
                //System.out.println("The keys are: "+keysArray1[i]);
                //System.out.println("The value of lookup inside is: "+airportLookupTable.get(keysArray1[i]));
                context.write(new Text(Integer.toString(i + 1) + ". " + airportLookupTable.get(keysArray1[i])), new Text("\t\t\tTotal delays in minutes: "+Integer.toString(sortedTotalDelay.get(keysArray1[i]))));
            }

            context.write(null, new Text("\n"));
            context.write(null, new Text("Top 10 Airports/Cities with highest average weather-realted delays:"));
            Set<String> keys2 = sortedAvgDelay.keySet();
            String[] keysArray2 = keys2.toArray(new String[keys2.size()]);
            for (int i = 0; i < keysArray2.length && i < 10; i++) {
                context.write(new Text(Integer.toString(i + 1) + ". " + airportLookupTable.get(keysArray2[i])), new Text("\t\t\tAvegrage Delay in minutes: "+Float.toString(sortedAvgDelay.get(keysArray2[i]))));
            }
        }
    }


