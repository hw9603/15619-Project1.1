package edu.cmu.scs.cc.project1;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Scanner;

public class WordCountSequentialReduce {

    /**
     * The reduce step to run the word count pipeline.
     *
     * <p>Note:
     * 1. The output file format is `word\tcount`, which is tab separated.
     * 2. Note that the encoding of input and output should be `utf-8`.
     *
     * You do not need to write any code to this file, however, you need
     * to figure out the intermediate Key-Value pair schema.
     *
     * @param args input args
     */
    public static void main(final String[] args) throws IOException {
        PrintWriter printWriter = new PrintWriter(
                new OutputStreamWriter(System.out, "UTF-8"), true);

        Scanner input = new Scanner(System.in, "UTF-8");
        String word = null;
        int count = 0;

        while (input.hasNextLine()) {
            try {
                String line = input.nextLine();
                String[] wordCount = line.split("\t");

                // the current word is equal to the previous one
                if (wordCount[0].equals(word)) {
                    count += Integer.parseInt(wordCount[1]);
                } else {
                    if (word != null) {
                        printWriter.println(word + "\t" + count);
                    }
                    count = Integer.parseInt(wordCount[1]);
                    word = wordCount[0];
                }

            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        // for the last word
        printWriter.println(word + "\t" + count);
    }
}
