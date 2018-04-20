package job5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

public class Job5Reducer2 extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {

        Integer totalPlaneInOperation = 0;
        Integer totalDelayInAllAgeTag = 0;
        HashMap<Integer, String> valuesCopy = new HashMap<>();
        for (Text val : values) {
            String[] elements = val.toString().split("\t");
            Integer age = Integer.parseInt(elements[0]);
            totalPlaneInOperation+= Integer.parseInt(val.toString().split("\t")[1]);
            totalDelayInAllAgeTag += Integer.parseInt(val.toString().split("\t")[2]);
            valuesCopy.put(age, val.toString().split("\t")[1]+"\t"+val.toString().split("\t")[2]);
        }
        context.write(new Text("Summary of Question 5"), null);
        context.write(null, new Text("\n"));
        context.write(null, new Text("Analysis of on-time performance per Plane-Age-Group"));
        Set<Integer> keys = valuesCopy.keySet();
        Integer[] keysArray = keys.toArray(new Integer[keys.size()]);
        context.write(new Text("SN\tAGE\tAGE-BUCKET\tTotal in Operation\t\tProportion of Operation(%)\t\tTotal Delays\t\tProportion of Delays(%)"), null);
        for (int i = 0; i < keysArray.length; i++) {
            Integer operationCount= Integer.parseInt(valuesCopy.get(keysArray[i]).split("\t")[0]);
            double operationProportion = operationCount * 100.0/totalPlaneInOperation;
            Integer totalDelay= Integer.parseInt(valuesCopy.get(keysArray[i]).split("\t")[1]);
            double delayProportion = totalDelay * 100.0/totalDelayInAllAgeTag;
            String ageBucket = keysArray[1]<20 ? "NEW" : "OLD" ;
            context.write(new Text(Integer.toString(i + 1) + ".\t" +keysArray[i]), new Text("\t"+ageBucket+"\t"+operationCount+
            "\t"+operationProportion+ " % "+"\t"+totalDelay+"\t"+delayProportion+" %"));
        }
    }
}
