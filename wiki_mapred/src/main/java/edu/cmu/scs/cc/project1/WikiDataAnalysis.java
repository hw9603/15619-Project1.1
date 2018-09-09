package edu.cmu.scs.cc.project1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce job workflow in hadoop for wiki data analysis.
 *
 * <p>TODO: Implement a MapReduce application in hadoop.
 *
 * <p>Hint: you could refer to the code in word count task.
 *
 * <p>All the steps below are described in a way aligned with Javadoc,
 * if you are unsure which methods to use, read the Javadoc of
 * {@link org.apache.hadoop.mapreduce.Job}.
 *
 * <p>1. Set the key/value class for the map output data.
 * 2. Set the key/value class for the job output data.
 * 3. Set the Mapper and Reducer class for the job.
 * 4. Set the number of Reducer tasks.
 * 5. Submit the job to the cluster and not wait for it to finish, i.e.
 * {@code job.waitForCompletion(true);}
 * 6. return the job, i.e.
 */
public class WikiDataAnalysis {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wiki analysis");
        job.setJarByClass(WikiDataAnalysis.class);
        job.setMapperClass(WikiMapper.class);
        job.setReducerClass(WikiReducer.class);
        job.setNumReduceTasks(1);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
