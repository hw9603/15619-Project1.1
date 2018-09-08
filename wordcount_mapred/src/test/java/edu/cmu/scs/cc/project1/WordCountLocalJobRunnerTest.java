package edu.cmu.scs.cc.project1;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

public class WordCountLocalJobRunnerTest {

    @Test
    public void run() throws Exception {
        Configuration conf = new Configuration();

        conf.set("mapred.job.tracker", "local");
        conf.set("fs.default.name", "file:///");

        Path inputPath = new Path("input");
        Path outputPath = new Path("output");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = runJob(conf, inputPath, outputPath);
        assertTrue(job.isSuccessful());
    }

    /**
     * Local job runner for word count mapreduce workflow.
     * @param conf hadoop configuration
     * @param inputPath input path of the local job runner
     * @param outputPath output path of the local job runner
     * @return the mapreduce job
     * @throws ClassNotFoundException if class-not-found exception occurs
     * @throws IOException if io exception occurs
     * @throws InterruptedException if interrupted exception occurs
     */
    public Job runJob(Configuration conf, Path inputPath, Path outputPath)
            throws ClassNotFoundException, IOException, InterruptedException {
        Job job = Job.getInstance(conf, "word count");

        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VIntWritable.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(false);
        return job;
    }
}