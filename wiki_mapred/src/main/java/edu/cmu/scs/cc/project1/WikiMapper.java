package edu.cmu.scs.cc.project1;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.InterruptedException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Mapper Utility.
 */
public class WikiMapper
        extends Mapper<Object, Text, Text, Text> {
    private Text countDatePair = new Text();
    private Text articleName = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException  {
        // Implement the code here for mapper of wiki data analysis,
        // you may need to change the key/value pair format as per your design
        PrintWriter err = new PrintWriter(
        new OutputStreamWriter(System.err, StandardCharsets.UTF_8), true);

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String filename = fileSplit.getPath().getName();
        String baseFilename = FilenameUtils.getBaseName(filename);
        // example for absoluteDate: 20180308
        int absoluteDate = Integer.parseInt(baseFilename.split("-")[1]);

        // need to convert to {0,1,2,...,30} for convenience
        // 20180308 -> 0, 20180406-> 29
        int dateIndex = (absoluteDate >= 20180401)
                        ? (absoluteDate - 20180401 + 24)
                        : (absoluteDate - 20180308);

        // err.println(value.toString() + "\n");
        String[] columns = DataFilter.getColumns(value.toString());
        // filter out invalid records
        // err.println(columns + "\n");
        if (!DataFilter.checkAllRules(columns)) return;
        // err.println("another hello?" + "\n");
        String result = columns[2];
        result += "\t" + dateIndex; // + operator silently convert int to string
        countDatePair.set(result);
        articleName.set(columns[1]);
        context.write(articleName, countDatePair);
    }
}
