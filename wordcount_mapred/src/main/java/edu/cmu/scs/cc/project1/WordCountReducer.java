package edu.cmu.scs.cc.project1;

import java.io.IOException;
import java.lang.InterruptedException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer
        extends Reducer<Text, VIntWritable, Text, VIntWritable> {
    private VIntWritable result = new VIntWritable();

    /**
     * The Reducer class to run the word count job.
     *
     * <p>TODO: Implement the reducer for word count.
     *
     * <p>Output (word, count) key/value pair.
     *
     * @param key input key of the reducer
     * @param values input value of the reducer
     * @param context output key/value pair of the reducer
     */
    public void reduce(Text key, Iterable<VIntWritable> values, Context context)
                        throws IOException, InterruptedException {
        int sum = 0;
        for (VIntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}