package job7;

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

public class Job7Reducer2 extends Reducer<Text,Text,Text,Text> {

    private HashMap<String, Integer> airportToCountDelayMap = new HashMap<>();
    private HashMap<String, Integer> airportToSumDelayMap = new HashMap<>();
    //private HashMap<String, Float> airportToAvgDelayMap = new HashMap<>();
    private HashMap<String, String> airportLookupTable = new HashMap<String, String>();
    private HashMap<String, String> planeLookupTable = new HashMap<String, String>();
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
                                String tailNumber = split[0].replaceAll("\"", "");
                                //System.out.println("The year is: "+split[8]);
                                //Integer builtYear = (split[8].contains("year")||split[8].contains("None")) ? 0 : Integer.parseInt(split[8]);
                                //String ageTag = ((2018 - builtYear) < 20) ? "NEW" : "OLD";
                                planeLookupTable.put(tailNumber, split[7].toString());
                                //System.out.println("The palne lookup buiding..key is: " + tailNumber);
                                //System.out.println("The plane lookup buiding..value is: " + split[7]);
                            }
                        }
                    }
                    else  if (str0.startsWith("Code") && str0.split(",").length == 2) {//System.out.println("get1");
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
            String airportID = elements[0];
            String carrierCode = elements[1];
            String tailNum = elements[2].toString();
            Integer numberOfDelay = Integer.parseInt(elements[3]);
            Integer totalDelay = Integer.parseInt(elements[4]);
            //Float avgDelay = Float.parseFloat(elements[5]);

            airportToCountDelayMap.put(airportID+"\t"+carrierCode+"\t"+tailNum, numberOfDelay);
            airportToSumDelayMap.put(airportID+"\t"+carrierCode+"\t"+tailNum, totalDelay);
            //airportToAvgDelayMap.put(airportID+"\t"+carrierCode+"\t"+tailNum, avgDelay);
        }
        Map<String, Integer> sortedCountDelay = util.sortByComparatorInteger(airportToCountDelayMap, false);
        Map<String, Integer> sortedTotalDelay = util.sortByComparatorInteger(airportToSumDelayMap, false);
       // Map<String, Float> sortedAvgDelay = util.sortByComparatorFloat(airportToAvgDelayMap, false);


        context.write(new Text("Summary of Question 7"), null);
        context.write(null, new Text("\n"));
        context.write(null, new Text("Top 10 Airports/Cities with most delays with detail about flight and its company:"));
        context.write(null, new Text("SN\tAirport\t\t\tCarrierName\t\t\tPlaneDetails\t\t\t\tNumofDelays"));
        Set<String> keys = sortedCountDelay.keySet();
        String[] keysArray = keys.toArray(new String[keys.size()]);
        for (int i = 0; i < keysArray.length && i < 10; i++) {
            context.write(new Text(Integer.toString(i + 1) + ". "
                            + airportLookupTable.get(keysArray[i].split("\t")[0])
                    +"\t\t\t"+carrierLookupTable.get(keysArray[i].split("\t")[1])
                    +"\t\t\t"+planeLookupTable.get(keysArray[i].split("\t")[2]))
                    , new Text("\t"+Integer.toString(sortedCountDelay.get(keysArray[i]))));
        }
        //System.out.println("The value of lookup is: "+airportLookupTable.get("ORD"));
        context.write(null, new Text("\n"));
        context.write(null, new Text("Top 10 Airports/Cities with sum total delays with detail about flight and its company:"));
        context.write(null, new Text("SN\tAirport\t\tCarrierName\t\t\tPlaneDetails\t\t\tTotalDelays"));
        Set<String> keys1 = sortedTotalDelay.keySet();
        String[] keysArray1 = keys1.toArray(new String[keys1.size()]);
        for (int i = 0; i < keysArray1.length && i < 10; i++) {
            //System.out.println("The keys are: "+keysArray1[i].split("\t")[2]);
            //System.out.println("The matched key gives: "+planeLookupTable.get("N804CA"));
            //System.out.println("The value of lookup inside is: "+airportLookupTable.get(keysArray1[i]));
            context.write(new Text(Integer.toString(i + 1) + ". "
                    + airportLookupTable.get(keysArray1[i].split("\t")[0])
                            +"\t\t\t"+carrierLookupTable.get(keysArray1[i].split("\t")[1])
                            +"\t\t\t"+planeLookupTable.get(keysArray1[i].split("\t")[2])),
                    new Text("\t"+Integer.toString(sortedTotalDelay.get(keysArray1[i]))));
        }

//        context.write(null, new Text("\n"));
//        context.write(null, new Text("Top 10 Airports/Cities with average delays with detail about flight and its company:"));
//        context.write(null, new Text("SN\tAirport\t\tCarrierName\t\t\tPlaneDetails\t\t\tAvgDelays"));
//        Set<String> keys2 = sortedAvgDelay.keySet();
//        String[] keysArray2 = keys2.toArray(new String[keys2.size()]);
//        for (int i = 0; i < keysArray2.length && i < 10; i++) {
//            context.write(new Text(Integer.toString(i + 1) + ". "
//                    + airportLookupTable.get(keysArray2[i].split("\t")[0])
//                    +"\t\t\t"+carrierLookupTable.get(keysArray2[i].split("\t")[1])
//                    +"\t\t\t"+planeLookupTable.get(keysArray2[i].split("\t")[2])),
//                    new Text("\t\t"
//                            +Float.toString(sortedAvgDelay.get(keysArray2[i]))));
//        }
    }
}


