package job1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Job1Comparator extends WritableComparator {
    protected Job1Comparator() {
        super(CompositeKeyWritable.class,true);
    }

    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
        CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

        int cmpResult = key1.getFirstElement().compareTo(key2.getFirstElement());
        if (cmpResult == 0)
        {
            return -1*key1.getSecondElement().compareTo(key2.getSecondElement());
        }
        return cmpResult;
    }
}