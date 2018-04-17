package job1;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

    //CompositeKey for Job1 (Year, totalDelay)
    public class CompositeKeyWritable implements Writable,
            WritableComparable<CompositeKeyWritable> {

        private String firstElement;
        private String secondElement;

        public CompositeKeyWritable() {
        }

        public CompositeKeyWritable(String firstElement, String secondElement) {
            this.firstElement = firstElement;
            this.secondElement = secondElement;
        }

        @Override
        public String toString() {
            return (new StringBuilder().append(firstElement).append("\t")
                    .append(secondElement)).toString();
        }

        public void readFields(DataInput dataInput) throws IOException {
            firstElement = WritableUtils.readString(dataInput);
            secondElement = WritableUtils.readString(dataInput);
        }

        public void write(DataOutput dataOutput) throws IOException {
            WritableUtils.writeString(dataOutput, firstElement);
            WritableUtils.writeString(dataOutput, secondElement);
        }

        public int compareTo(CompositeKeyWritable objKeyPair) {
            // TODO:
            /*
             * Note: This code will work as it stands; but when CompositeKeyWritable
             * is used as key in a map-reduce program, it is de-serialized into an
             * object for comapareTo() method to be invoked;
             *
             * To do: To optimize for speed, implement a raw comparator - will
             * support comparison of serialized representations
             */
            int result = firstElement.compareTo(objKeyPair.firstElement);
            if (0 == result) {
                result = secondElement.compareTo(objKeyPair.secondElement);
            }
            return result;
        }

        public String getFirstElement() {
            return firstElement;
        }

        public void setFirstElement(String firstElement) {
            this.firstElement = firstElement;
        }

        public String getSecondElement() {
            return secondElement;
        }

        public void setSecondElement(String secondElement) {
            this.secondElement = secondElement;
        }
    }

