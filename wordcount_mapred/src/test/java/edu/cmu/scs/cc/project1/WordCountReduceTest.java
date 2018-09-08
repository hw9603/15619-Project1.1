package edu.cmu.scs.cc.project1;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountReduceTest {

    private Reducer<Text, VIntWritable, Text, VIntWritable> reducer;
    private ReduceDriver<Text, VIntWritable, Text, VIntWritable> driver;

    /**
     * Setup the reducer for word count.
     */
    @Before
    public void setUp() {
        reducer = new WordCountReducer();
        driver = new ReduceDriver<>(reducer);
    }

    /**
     * {@code ReduceDriver.runTest(false)}: the order does not matter.
     *
     * @throws IOException if io exception occurs
     */
    @Test
    public void testWordCountReducer() throws IOException {
        driver.withInput(new Text("foo"), Arrays.asList(new VIntWritable(1), new VIntWritable(1)))
                .withInput(new Text("bar"), Arrays.asList(new VIntWritable(1)))
                .withOutput(new Text("bar"), new VIntWritable(1))
                .withOutput(new Text("foo"), new VIntWritable(2))
                .runTest(false);
    }
}
