package edu.cmu.scs.cc.project1;

import java.io.IOException;
import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class WikiMapTest extends TestCase {
    private Mapper<Object, Text, Text, Text> mapper;
    private MapDriver<Object, Text, Text, Text> driver;

    /**
     * Setup the mapper for word count.
     */
    @Before
    public void setUp() {
        mapper = new WikiMapper();
        driver = new MapDriver<>(mapper);
    }

    @Test
    public void testWikiMapper() throws IOException  {
        // throw new RuntimeException("add map test cases on your own");
        driver.withMapInputPath(
            new Path("/home/clouduser/Project1/wiki_mapred/input/pageview-20180308-000000.gz"))
            .withInput(new Text(""), new Text("en Apple 100000 0")) // set the input records
            .withInput(new Text(""), new Text("en Apple 7 0"))
            .withInput(new Text(""), new Text("en Banana 100000 0"))
            .withInput(new Text(""), new Text("en Banana 1 0"))
            .withInput(new Text(""), new Text("en Apple 1 0"))
            .withInput(new Text(""), new Text("en Carnegie_Mellon_University 200000 0"))
            .withOutput(new Text("Apple"), new Text("100000\t0"))
            .withOutput(new Text("Apple"), new Text("7\t0"))
            .withOutput(new Text("Banana"), new Text("100000\t0"))
            .withOutput(new Text("Banana"), new Text("1\t0"))
            .withOutput(new Text("Apple"), new Text("1\t0"))
            .withOutput(new Text("Carnegie_Mellon_University"), new Text("200000\t0"))
            .runTest(false);
    }
}
