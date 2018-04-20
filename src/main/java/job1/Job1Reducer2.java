package job1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.DeflaterOutputStream;

public class Job1Reducer2 extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text();
    HashMap<String, Float> hourToDelayMap = new HashMap<>();
    HashMap<String, Float> dayInWeekToDelayMap = new HashMap<>();
    HashMap<String, Float> monthInYearToMap = new HashMap<>();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {

        CustomUtility util = new CustomUtility();
        for (Text result : values) {
            String[] elements = result.toString().split("\t");
            String timeTag = elements[0].toString().substring(1);
            Integer delayCount = Integer.parseInt(elements[1]);
            Integer sumDelay = Integer.parseInt(elements[2]);
            Float avgDelay = Float.parseFloat(elements[3]);
            if (elements[0].charAt(0) == 'H')
                hourToDelayMap.put(timeTag, avgDelay);
            else if (elements[0].charAt(0) == 'W')
                dayInWeekToDelayMap.put(timeTag, avgDelay);
            else if (elements[0].charAt(0) == 'M')
                monthInYearToMap.put(timeTag, avgDelay);
        }

        Map<String, Float> sortedHourlyDelay = util.sortByComparatorFloat(hourToDelayMap, false);
        Map<String, Float> sortedWeeklyDelay = util.sortByComparatorFloat(dayInWeekToDelayMap, false);
        Map<String, Float> sortedMonthlyDelay = util.sortByComparatorFloat(monthInYearToMap, false);


        context.write(new Text("Summary of Question 1 & 2"), null);
        context.write(null, new Text("\n"));
        context.write(null, new Text("Top 10 Worst Time of Day to travel:"));
        Set<String> keys = sortedHourlyDelay.keySet();
        String[] keysArray = keys.toArray(new String[keys.size()]);
        for (int i = 0; i < keysArray.length && i < 10; i++) {
            context.write(new Text(Integer.toString(i + 1) + ". " + keysArray[i]), new Text("\t\t\tAvg delay occured: " + Float.toString(sortedHourlyDelay.get(keysArray[i]))));
        }
        context.write(null, new Text("\n"));
        context.write(null, new Text("Top 10 Best Time of Day to travel:"));
        for (int i = 1; i <= 10; i++) {
            context.write(new Text(Integer.toString(i) + ". " + keysArray[keysArray.length - i]), new Text("\t\t\tAvg delay occured: " + Float.toString(sortedHourlyDelay.get(keysArray[keysArray.length - i]))));
        }

        context.write(null, new Text("\n"));
        context.write(null, new Text("Top 7 Worst Day of Week to travel:"));
        Set<String> keys1 = sortedWeeklyDelay.keySet();
        String[] keysArray1 = keys1.toArray(new String[keys1.size()]);
        for (int i = 0; i < keysArray1.length && i < 10; i++) {
            context.write(new Text(Integer.toString(i + 1) + ". " + keysArray1[i]), new Text("\t\t\tAvg delay occured: " + Float.toString(sortedWeeklyDelay.get(keysArray1[i]))));
        }
        context.write(null, new Text("\n"));
        context.write(null, new Text("Top 7 Best Day of Week to travel:"));
        for (int i = 1; i < keysArray1.length && i <= 10; i++) {
            context.write(new Text(Integer.toString(i) + ". " + keysArray1[keysArray1.length - i]), new Text("\t\t\tAvg delay occured: " + Float.toString(sortedWeeklyDelay.get(keysArray1[keysArray1.length - i]))));
        }

        context.write(null, new Text("\n"));
        context.write(null, new Text("Top 10 Worst Month of Year to travel:"));
        Set<String> keys2 = sortedMonthlyDelay.keySet();
        String[] keysArray2 = keys2.toArray(new String[keys2.size()]);
        for (int i = 0; i < keysArray2.length && i < 10; i++) {
            context.write(new Text(Integer.toString(i + 1) + ". " + keysArray2[i]), new Text("\t\t\tAvg delay occured: " + Float.toString(sortedMonthlyDelay.get(keysArray2[i]))));
        }
        context.write(null, new Text("\n"));
        context.write(null, new Text("Top 10 Best Month of Year to travel:"));
        for (int i = 1;i < keysArray2.length && i <= 10; i++) {
            context.write(new Text(Integer.toString(i) + ". " + keysArray2[keysArray2.length - i]), new Text("\t\t\tAvg delay occured: " + Float.toString(sortedMonthlyDelay.get(keysArray2[keysArray2.length - i]))));
        }
    }
}