package edu.cmu.scs.cc.project1;

import java.io.IOException;
import java.lang.InterruptedException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper
        extends Mapper<Object, Text, Text, VIntWritable> {
    private static final VIntWritable one = new VIntWritable(1);
    private Text word = new Text();

    /**
     * The Mapper class to run the word count job.
     *
     * <p>TODO: Implement the map method
     *
     * <p>Output (word, 1) key/value pair.
     *
     * <p>Hint:
     * StringTokenizer is faster than String.split()
     *
     * @param key input key of the mapper
     * @param value input value of the mapper
     * @param context output key/value pair of the mapper
     */
    public void map(Object key, Text value, Context context)
                    throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}