package edu.cmu.scs.cc.project1;

import java.io.IOException;
import java.lang.InterruptedException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer utility.
 */
public class WikiReducer
        extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    private Text totalViews = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context
                        ) throws IOException, InterruptedException {
        // Implement the code here for reducer of wiki data analysis,
        // you may need to change the key/value pair format as per your design
        int totalMonthViews = 0;
        int[] dailyViews = new int[30];
        for (Text val : values) {
            String valString = val.toString();
            String[] countDate= valString.split("\t");
            int count = 0;
            try {
                count = Integer.parseInt(countDate[0]);
            } catch (NumberFormatException e) {
                // ignore this exception and continue processing following input lines
            }

            int date = Integer.parseInt(countDate[1]);
            dailyViews[date] += count;
            totalMonthViews += count;
        }
        if (totalMonthViews <= 100000) return;

        String pageViews = "";
        for (int view : dailyViews) {
            pageViews += view + "\t";
        }
        // remove the last tab
        pageViews = pageViews.substring(0, pageViews.length() - 1);
        pageViews = key.toString() + "\t" + pageViews;
        result.set(pageViews);
        totalViews.set(String.valueOf(totalMonthViews));
        context.write(totalViews, result);
    }
}
