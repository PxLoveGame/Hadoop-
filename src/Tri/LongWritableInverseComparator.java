package Tri;

import org.apache.hadoop.io.LongWritable;

/**
 * Tri inverse pour les clés de type LongWritable
 */
public class LongWritableInverseComparator extends InverseComparator<LongWritable> {
    public LongWritableInverseComparator() {
        super(LongWritable.class);
    }
}
