package job5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Job5Mapper1 extends Mapper<Object, Text, Text, Text> {

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
                            String airportID = split[0].replaceAll("\"", "");
                            String airportName = split[1].replaceAll("\"", "");
                            String cityName = split[2].replaceAll("\"", "");
                            String stateName = split[3].replaceAll("\"", "");
                            airportLookupTable.put(airportID, (airportName + "," + cityName + "," + stateName));
                            }
                    }else if (str0.startsWith("tailnum") && str0.split(",").length == 9) {//System.out.println("get1");
                        String str = null;
                        while ((str = br.readLine()) != null) {
                            String[] split = str.split(",");
                            if (split.length == 9) {
                                String tailNumber = split[0];
                                //System.out.println("The year is: "+split[8]);
                                if((!split[8].contains("year")&&!split[8].contains("None"))) {
                                    Integer builtYear = Integer.parseInt(split[8]);
                                    Integer planeAge = 2009 - builtYear; //newest airplane can be built by 2008
                                    String ageTag = (planeAge < 20) ? "NEW" : "OLD";
                                    if(planeAge!=2009)
                                        planeLookupTable.put(tailNumber, (planeAge.toString() + "\t" + ageTag));
                                }
                                //System.out.println("The palne lookup buiding..key is: " + tailNumber);
                                //System.out.println("The plane lookup buiding..value is: " + ageTag);
                            }
                        }
                    }
                }
            }
        }
    }

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String eachLine = value.toString();
        if(!eachLine.startsWith("Year") && !eachLine.startsWith("NA")){
            String [] elements= eachLine.split(",");
            String tailNumber = elements[10].equals("NA") ? "0": elements[10];
            //System.out.println("The tailNumber in map() is: "+tailNumber);
            String arrDelay = elements[14].equals("NA") ? "0": elements[14];
            String depDelay = elements[15].equals("NA") ? "0": elements[15];
            Integer totalDelay = Integer.parseInt(arrDelay)+Integer.parseInt(depDelay);
            if(planeLookupTable.get(tailNumber)!=null){
                String age = planeLookupTable.get(tailNumber).split("\t")[0];
                String ageTag = planeLookupTable.get(tailNumber).split("\t")[1];
                //System.out.println("The age in map() is: "+age);
                //System.out.println("The ageTag in map() is: "+ageTag);
                context.write(new Text(age), new Text(totalDelay.toString()+"\t"+"1"));
            }
        }
    }
}