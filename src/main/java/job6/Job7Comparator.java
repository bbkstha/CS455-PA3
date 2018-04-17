package job6;

import job1.CompositeKeyWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Job7Comparator extends WritableComparator {
    protected Job7Comparator() {
        super(CompositeKeyWritable.class,true);
    }

    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
        CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
        Integer firstElementKey1 = Integer.parseInt(key1.getFirstElement().substring(1));
        Integer firstElementKey2 = Integer.parseInt(key2.getFirstElement().substring(1));

        Float secondElemetKey1 = Float.parseFloat(key1.getSecondElement());
        Float secondElemetKey2 = Float.parseFloat(key2.getSecondElement());

        if(key1.getFirstElement().charAt(0)=='*'){
            int cmpResult = firstElementKey1.compareTo(firstElementKey2);
            return -1*cmpResult;
        }
        else if(key1.getFirstElement().charAt(0)=='^'){

            int cmpResult = secondElemetKey1.compareTo(secondElemetKey2);
            return -1*cmpResult;
        }
        return 0;
    }
}