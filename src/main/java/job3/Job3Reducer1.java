package job3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job3Reducer1 extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        int counter = 0;
        for (Text val : values) {
            counter+=Integer.parseInt(val.toString());
        }
        result.set(Integer.toString(counter));
        context.write(key, result);
    }
}
