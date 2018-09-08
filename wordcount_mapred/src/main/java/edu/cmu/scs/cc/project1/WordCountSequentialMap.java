package edu.cmu.scs.cc.project1;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Scanner;
import java.util.StringTokenizer;

public class WordCountSequentialMap {

    /**
     * The map step to run the word count pipeline.
     *
     * <p>Note:
     * The encoding of input and output should be `utf-8`.
     *
     * @param args input args
     */
    public static void main(final String[] args) throws IOException {
        Scanner input = new Scanner(System.in, "UTF-8");

        PrintWriter printWriter = new PrintWriter(
                new OutputStreamWriter(System.out, "UTF-8"), true);
        while (input.hasNextLine()) {
            String line = input.nextLine();
            try {
                StringTokenizer itr = new StringTokenizer(line);
                while (itr.hasMoreTokens()) {
                    // throw new RuntimeException("To be implemented");
                    printWriter.println(itr.nextToken() + "\t1");
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}




