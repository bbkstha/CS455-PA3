package job5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job5Reducer1 extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text();
    @Override
    public void reduce(Text key, Iterable<Text> values,
                   Context context
        ) throws IOException, InterruptedException {


        int counter=0;
        int totalDelay=0;
        for(Text val:values){
            //firstChar = val.toString().charAt(0);
            totalDelay+=Integer.parseInt(val.toString().split("\t")[0]);
            counter+=Integer.parseInt(val.toString().split("\t")[1]);
        }
        result.set(Integer.toString(counter)+"\t"+Integer.toString(totalDelay));
        context.write(key, result);

    }
}
