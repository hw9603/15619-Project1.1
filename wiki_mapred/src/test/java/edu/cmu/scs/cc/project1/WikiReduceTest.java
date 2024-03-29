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
        driver.withInput(new Text("Apple"),
                    Arrays.asList(new Text("100000\t0"), new Text("7\t0"), new Text("1\t0")))
                .withInput(new Text("Banana"),
                    Arrays.asList(new Text("100000\t0"), new Text("1\t0")))
                .withInput(new Text("Carnegie_Mellon_University"),
                    Arrays.asList(new Text("200000\t0")))
                .withOutput(new Text("100008"),
                    new Text("Apple\t100008\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0"
                                + "\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0"))
                .withOutput(new Text("100001"),
                    new Text("Banana\t100001\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0"
                                + "\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0"))
                .withOutput(new Text("200000"),
                    new Text("Carnegie_Mellon_University\t200000\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0"
                                + "\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0"))
                .runTest(false);
    }
}
