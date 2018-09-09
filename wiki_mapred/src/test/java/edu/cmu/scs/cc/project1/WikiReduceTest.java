package edu.cmu.scs.cc.project1;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WikiReduceTest {
    private Reducer<Text, Text, Text, Text> reducer;
    private ReduceDriver<Text, Text, Text, Text> driver;

    @Before
    public void setUp() {
        reducer = new WikiReducer();
        driver = new ReduceDriver<>(reducer);
    }

    @Test
    public void testWikiReducer() throws IOException {
        driver.withInput(new Text("Apple"), Arrays.asList(new Text("10000\t0"), new Text("7\t0"), new Text("1\t0")))
                .withInput(new Text("Banana"), Arrays.asList(new Text("10000\t0"), new Text("1\t0")))
                .withInput(new Text("Carnegie_Mellon_University"), Arrays.asList(new Text("20000\t0")))
                .withOutput(new Text("10008"), new Text("Apple\t10008\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0")) // set the expected output records
                .withOutput(new Text("10001"), new Text("Banana\t10001\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0"))
                .withOutput(new Text("20000"), new Text("Carnegie_Mellon_University\t20000\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0"))
                .runTest(false);
    }
}
