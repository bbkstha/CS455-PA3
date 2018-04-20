package job4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job4Combiner1 extends Reducer<Text,Text,Text,Text> {

public void reduce(Text key,Iterable<Text> values,
        Context context
        )throws IOException,InterruptedException{

        int counter=0;
        int totalDelay=0;
        for(Text val:values){
        //firstChar = val.toString().charAt(0);
        totalDelay+=Integer.parseInt(val.toString().split("\t")[0]);
        counter+=Integer.parseInt(val.toString().split("\t")[1]);
        }

        context.write(key, new Text(totalDelay+"\t"+counter));

        }

}