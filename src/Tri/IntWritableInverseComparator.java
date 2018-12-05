package Tri;

import org.apache.hadoop.io.IntWritable;

public class IntWritableInverseComparator extends InverseComparator<IntWritable> {
    public IntWritableInverseComparator() {
        super(IntWritable.class);
    }
}
