package Tri;

import org.apache.hadoop.io.LongWritable;

public class LongWritableInverseComparator extends InverseComparator<LongWritable> {
    public LongWritableInverseComparator() {
        super(LongWritable.class);
    }
}
