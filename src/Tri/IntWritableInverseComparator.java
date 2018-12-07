package Tri;

import org.apache.hadoop.io.IntWritable;

/**
 * Tri inverse pour les clés de type IntWritable
 */
public class IntWritableInverseComparator extends InverseComparator<IntWritable> {
    public IntWritableInverseComparator() {
        super(IntWritable.class);
    }
}
