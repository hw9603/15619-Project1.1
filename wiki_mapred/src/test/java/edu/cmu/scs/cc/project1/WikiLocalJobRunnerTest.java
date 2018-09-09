package edu.cmu.scs.cc.project1;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

public class WikiLocalJobRunnerTest {

    /**
     * Run the local MapReduce job using the local secret dataset.
     *
     * WARNING: DO NOT change this method
     *
     * @throws Exception if error occurs
     */
    @Test
    public void run() throws Exception {

        Configuration conf = new Configuration();

        conf.set("mapred.job.tracker", "local");
        conf.set("fs.default.name", "file:///");

        Path inputPath = new Path("/home/ubuntu/cmucc-dataset/");
        Path outputPath = new Path("output");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = runJob(conf, inputPath, outputPath);
        assertTrue(job.isSuccessful());

    }

    /**
     * Local job runner for wiki big data analysis mapreduce workflow.
     *
     * <p>TODO: Configure the MapReduce job.
     *
     * <p>All the steps below are described in a way aligned with Javadoc,
     * if you are unsure which methods to use, read the Javadoc of
     * {@link org.apache.hadoop.mapreduce.Job}.
     *
     * <p>1. Set the key/value class for the map output data.
     * 2. Set the key/value class for the job output data.
     * 3. Set the Mapper and Reducer class for the job.
     * 4. Submit the job to the cluster and not wait for it to finish, i.e.
     * {@code job.waitForCompletion(false);}
     * 5. return the job, i.e.
     * {@code return job;}
     *
     * @param conf       hadoop configuration
     * @param inputPath  input path of local job runner
     * @param outputPath output path of local job runner
     * @return mapreduce job
     * @throws ClassNotFoundException if class-not-found exception found
     * @throws IOException            if IO error found
     * @throws InterruptedException   if interrupted exception found
     */
    public Job runJob(Configuration conf, Path inputPath, Path outputPath)
            throws ClassNotFoundException, IOException, InterruptedException {
        Job job = Job.getInstance(conf, "wiki");

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(WikiMapper.class);
        job.setReducerClass(WikiReducer.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(false);
        return job;
    }

}
