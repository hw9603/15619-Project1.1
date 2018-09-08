package edu.cmu.scs.cc.project1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper Utility.
 */
public class WikiMapper
        extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context)  {
        // Implement the code here for mapper of wiki data analysis,
        // you may need to change the key/value pair format as per your design
    }
}
