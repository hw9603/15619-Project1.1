#!/bin/bash

################################################################################
##                       Cloud Computing Course                               ##
##                    Runner Script for Project 1                             ##
##                                                                            ##
##            Copyright 2018-2019: Cloud Computing Course                     ##
##                     Carnegie Mellon University                             ##
##   Unauthorized distribution of copyrighted material, including             ##
##  unauthorized peer-to-peer file sharing, may subject the students          ##
##  to the fullest extent of Carnegie Mellon University copyright policies.   ##
################################################################################

################################################################################
##                      README Before You Start                               ##
################################################################################
# Fill in the functions below for each question.
# You may use any programming language(s) in any question.
# You may use other files or scripts in these functions as long as they are in
# the submission folder.
# All files MUST include source code (e.g. do not just submit jar or pyc files).
#
# We will suggest tools or libraries in each question to enrich your learning.
# You are allowed to solve questions without the recommended tools or libraries.
#
# The colon `:` is a POSIX built-in basically equivalent to the `true` command,
# REPLACE it with your own command in each function.
# Before you fill your solution,
# DO NOT remove the colon or the function will break because the bash functions
# may not be empty!


################################################################################
##                          Setup & Cleanup                                   ##
################################################################################

setup() {
  # Fill in this helper function to do any setup if you need to.
  #
  # This function will be executed once at the beginning of the grading process.
  # Other functions may be executed repeatedly in arbitrary order.
  # Make use of this function to reduce unnecessary overhead and to make your
  # code behave consistently.
  #
  # e.g. You should compile Java code in this function.
  #
  # However, DO NOT use this function to solve any question or
  # generate any intermediate output.
  #
  # Examples:
  # mvn clean package
  # pip3 install -r requirements.txt
  #
  # Standard output format:
  # No standard output needed
  mvn package
}

cleanup() {
  # Fill this helper function to clean up if you need to.
  #
  # This function will be executed once at the end of the grading process
  # Other functions might be executed repeatedly in arbitrary order.
  #
  # Examples:
  # mvn clean
  #
  # Standard output format:
  # No standard output needed
  mvn clean
}

################################################################################
##                        Data Pre-processing                                 ##
################################################################################

filter() {
  # (20 points)
  #
  # Question:
  # Fill this function to filter the input stream read from the dataset `pageviews-20180310-000000.gz`
  # and redirect the output stream to a file called `output`.
  # You need to read the gz file directly as the input stream, instead of
  # uncompressing and then reading it.
  # You need to redirect the standard output stream to a file named `output`,
  # instead of using File I/O.
  #
  # One possible approach is to use `zcat` to uncompress the gz file to StdOut
  # and pipe it to your data filter program.
  #
  # Examples:
  # zcat pageviews-20180310-000000.gz | java -cp target/project1.jar edu.cmu.scs.cc.project1.DataFilter
  #
  # Hint for Java users:
  # DO NOT compile java code in this function,
  # you should put the `mvn` or `javac` command in the setup() function
  #
  # Standard output format:
  # No standard output needed
  #
  # Note: the program must be encoding aware
  zcat pageviews-20180310-000000.gz | java -cp target/project1.jar edu.cmu.scs.cc.project1.DataFilter
}

################################################################################
##                      Remote Debugging Basics                               ##
################################################################################
# Debugging a distributed application remotely is not as straightforward as
# debugging a local application. The following questions will demonstrate some
# basic methods to search information from logs and debug remotely with GNU
# tools. Suppose the log file is obtained after enabling the log aggregation
# property in configuration.

q1() {
  # (1 points) search a substring in a file with grep
  #
  # Scenario:
  # Suppose you wrote a Java program on a Hadoop MapReduce framework but the
  # program failed with error code 1.
  # This error code indicated that your program was likely to have bugs.
  # You connected to the master core nodes, obtained the log file with
  # `yarn logs -applicationId <application_Id> > mapreduce_log`.
  #
  # Question:
  # Search the log file and print the lines that contain "Exception" as a substring.
  #
  # `grep`, named after "Global/Regular Expression/Print", prints lines that
  # contain a match for a pattern.
  # Try searching the options of GNU grep from the manual page,
  # e.g. by using grep itself: `man grep | grep recursive`.
  #
  # Now you can solve the question using:
  # grep options pattern input_file
  #
  # Standard output format:
  # <input_filename>:<matched_line>
  # <input_filename>:<matched_line>
  # ...
  grep "Exception" mapreduce_log
}

q2() {
  # (2 points) options in grep
  #
  # Scenario:
  # A MapReduce Program, as its distributed feature, often produces duplicate
  # error messages among containers.
  # The previous solution can produce a large output as the size of the
  # logs grows, and you are more interested in unique exceptions.
  #
  # Question:
  # Print the unique exceptions in `mapreduce_log` in byte-wise order.
  # We ASSUME the exception format is a contiguous non-empty sequence of word
  # characters or dots(.), followed by exactly "Exception"
  # i.e. "(at least 1 word character or dot)Exception(a word boundary)"
  # e.g. "java.lang.NullPointerException"
  #
  # Search the options of GNU grep that:
  # 1. print only the matched parts instead of whole matching lines
  # 2. interpret the pattern as a Perl regex for additional functionality
  #
  # Here are some clues related to the syntax of regular expressions in Perl:
  # 1. find the regex operator that represents word characters, which is
  # supported by Perl regex
  # 2. explore the concept of word boundaries and find its regex operator
  #
  # Finally, sort the `grep` output by piping the StdOut to `sort` with its
  # option that indicates "unique" to remove duplicates:
  # grep options 'regex' input_file | sort options
  #
  # Standard output format:
  # <package.ExceptionName>
  # <package.ExceptionName>
  # ...
  grep -oP '([\w.|.]+)Exception\b' mapreduce_log | sort -u
}

q3() {
  # (3 points) search with context
  #
  # Scenario:
  # Among the exceptions you found, "java.lang.NullPointerException" is the most
  # likely root cause of error in MR jobs.
  # You want to look into the context, i.e. the Java stack trace.
  #
  # A stack trace is a list of the names of the classes and methods that were
  # called at the point when the exception occurred.
  #
  # For example:
  # Exception in thread "main" java.lang.NullPointerException
  #   at INeverCareAboutEdgeCases.whyBother(INeverCareAboutEdgeCases.java:241)
  #     (tens of trace elements omitted)
  #   at MarsClimateOrbiter.move(MarsClimateOrbiter.java:96)
  #   at MarsClimateOrbiter.main(MarsClimateOrbiter.java:72)
  #
  # You want to find the lines containing "java.lang.NullPointerException" with
  # the trailing lines to get the full stack trace.
  #
  # Question:
  # Print the lines that contain "java.lang.NullPointerException" in the log,
  # along with the next 8 lines of trailing context after each group of matches.
  # Separate contiguous groups of matches with a line containing a group
  # separator (--).
  #
  # Search the options of GNU grep that:
  # Print N lines of trailing context after matching lines, and places a line
  # containing a group separator (--) between contiguous groups of matches.
  #
  # Standard output format:
  # <the matching line of NullPointerException>
  # <the 1st line of context>
  # <the 2nd line of context>
  # ...
  # <the 8th line of context>
  # --
  # <the matching line of NullPointerException>
  # <the 1st line of context>
  # <the 2nd line of context>
  # ...
  # <the 8th line of context>
  # ...
  grep -A 8 'java.lang.NullPointerException' mapreduce_log
}

################################################################################
##                           Data Analysis                                    ##
################################################################################
# In the following sections, you should write a function to compute the answer
# and print it to StdOut for each question.
#
# Finish the related primers first if you have not done so, as they will help
# you to get well prepared for the questions.
#
# WARNING:
# Treat every function as an independent and standalone program.
# Any question below should not depend on the result or the intermediate
# output from another.

# Note: the input file in the following questions can only
# be `output` for submission!!! You can use other names while testing
# in the AMI.

q4() {
  # (1 points) read a TSV file to a DataFrame and calculate descriptive
  # statistics
  #
  # Question:
  # Print column-wise descriptive statistics of the output file.
  #
  # 1. the statistics metrics required are as follows (in a tabular view):
  #       monthly_view  daily_view  daily_view ...  daily_view
  # count     ...           ...         ...             ...
  # mean      ...           ...         ...             ...
  # std       ...           ...         ...             ...
  # min       ...           ...         ...             ...
  # 25%       ...           ...         ...             ...
  # 50%       ...           ...         ...             ...
  # 75%       ...           ...         ...             ...
  # max       ...           ...         ...             ...
  # 2. the statistics result should be in CSV format
  # 3. format each value to 2 decimal places
  #
  # A DataFrame represents a tabular, spreadsheet-like data structure containing
  # an ordered collection of columns.
  # As you notice, there is an intuitive relation between DataFrame and TSV
  # files.
  #
  # Use the pandas lib to read a TSV file and generate descriptive statistics
  # without having to reinvent the wheel.
  #
  # Hints:
  # 1. pandas.read_table can read a TSV file and return a DataFrame
  # 2. be careful that you should not read the first record as the header
  # 3. figure out the method of pandas.DataFrame which can calculate
  # descriptive statistics
  # 4. figure out the parameter of pandas.DataFrame.to_csv to format the
  # floating point numbers.
  #
  # You can solve the problem by filling in the correct method or parameters
  # to the following Python code:
  #
  # import pandas
  # df = pandas.read_table('output', index_col=1, encoding='utf-8', header=None)
  # df.describe().to_csv(sys.stdout, encoding='utf-8', float_format='%.2f')
  #
  # `index_col=1` means the second column of the input file, i.e. `page_title`
  # will be read as indexes.
  #
  # Standard output format:
  # count,5422.00,5422.00,...,5422.00
  # mean,210454.72,7787.50,...,6959.28
  # std,267435.68,14333.08,...,10597.68
  # min,100001.00,0.00,...,0.00
  # 25%,117623.50,3584.00,...,3477.00
  # 50%,149564.00,4962.00,...,4657.00
  # 75%,217951.25,7745.75,...,7126.50
  # max,12220791.00,432462.00,...,342737.00
  python3 q4.py
}

q5() {
  # (1 points) introduction to Series
  #
  # Question:
  # Print the 85th percentile of daily pageviews on March 10.
  # Note: the data starts on March 8.
  #
  # The other basic Data Structure in pandas is Series.
  #
  # 1. a DataFrame can be thought of as a dictionary of Series:
  # each column is a Series and all the Series share the same index.
  # 2. Series is a one-dimensional array-like object containing an array of data
  # and an associated array of data labels, called its index.
  #
  # The workflow to solve this question can be as follows:
  # 1. read the file as a DataFrame
  # 2. select the column of March 10
  # 3. calculate the 85th percentile
  #
  # In this question, you will explore one of the approaches to get a column
  # from a DataFrame: integer position based selection (iloc).
  #
  # Read the "Selection By Position" section at:
  # https://pandas.pydata.org/pandas-docs/stable/indexing.html
  #
  # You can solve the problem by filling in the correct positions or parameters
  # to the following Python code:
  #
  # <import libs on your own>
  # df = pandas.read_table('output', index_col=1, encoding='utf-8', <params>)
  # print("%.2f" % df.iloc[<positions>].<method>)
  #
  # Standard output format:
  # <a decimal value with 2 decimal places>
  python3 q5.py
}

q6() {
  # (1 points) the essence of a Series is a dictionary
  #
  # Question:
  # Print the first 5 records in the output file along with monthly pageviews
  # as a JSON object.
  # The keys are the articles and the values are the pageviews.
  #
  # A series can be thought of a dictionary/map in essence.
  # A dictionary/map can be represented as a JSON object, which is an unordered
  # set of name/value pairs.
  #
  # Hint:
  # 1. use pandas.DataFrame.iloc[pos] to select the monthly_view column as a
  # Series
  # 2. the naming of the method which converts a pandas.Series to JSON is
  # self-explanatory
  # 3. to select the first 5 records, you can choose one of the following
  # approaches:
  # 3.1. use a method of pandas.Series similar to GNU tool `head` which
  # returns the first n rows
  # 3.2. use pandas.Series.iloc[pos]
  #
  # Standard output format:
  # {"Stephen_Hawking":12220791,...,"Deaths_in_2018":3076157}
  python3 q6.py
}

q7() {
  # (1 points) rank DataFrame and break ties
  #
  # Question:
  # `teams.txt` has a list of articles about NCCA men's basketball teams. Please
  # rank the men's basketball teams by the monthly pageviews. Print the ranked
  # result as a JSON object.
  #
  # The keys of the JSON object are the articles, and the values are the
  # rankings (not pageviews).
  #
  # If the name of men's basketball team does not exist in `output`, do
  # not include it in the ranking.
  #
  # Use "dense ranking" to deal with ties, a.k.a "1223" ranking.
  # If A ranks ahead of B and C (which compare equal) and both ranked ahead of
  # D, then A gets ranking number 1 ("first"), B gets ranking number 2
  # ("joint second"), C also gets ranking number 2 ("joint second") and D gets
  # ranking number 3 ("third").
  # Reference: https://en.wikipedia.org/wiki/Ranking
  #
  # There is no need to (re)implement your own ranking algorithm.
  # pandas.DataFrame.rank enables users to choose from different ways to
  # assign a rank to tied elements.
  #
  # One possible solution is as follows:
  #
  # <import libs on your own>
  # monthly_view = pandas.read_table('output', index_col=1, encoding='utf-8', \
  # <params>).iloc[<positions>]
  # pandas.read_table('teams.txt', index_col=0, encoding='utf-8').join( \
  # monthly_view, how='inner')
  # print(monthly_view.rank(<params>).<methods>.to_json())
  #
  # Standard output format:
  # {"UMBC_Retrievers_men's_basketball":3,...,"Gonzaga_Bulldogs_men's_basketball":6}
  python3 q7.py
}

################################################################################
##                    DO NOT MODIFY ANYTHING BELOW                            ##
################################################################################
declare -ar questions=( "filter" "q1" "q2" "q3" "q4" "q5" "q6" "q7" )
declare -ar mapred=( "map" "reduce" )

readonly usage="This program is used to execute your solution.
Usage:
./runner.sh to run all the questions
./runner.sh -r <question_id> to run one single question
./runner.sh -s to run setup() function
./runner.sh -c to run cleanup() function
Example:
./runner.sh -r q1 to run q1"

contains() {
  local e
  for e in "${@:2}"; do
    [[ "$e" == "$1" ]] && return 0;
  done
  return 1
}

while getopts ":hr:sc" opt; do
  case ${opt} in
    h)
      echo "$usage" >&2
      exit
    ;;
    s)
      setup
      echo "setup() function executed" >&2
      exit
    ;;
    c)
      cleanup
      echo "cleanup() function executed" >&2
      exit
    ;;
    r)
      question=$OPTARG
      if contains "$question" "${questions[@]}"; then
        answer=$("$question")
        echo -n "$answer"
      else
        if contains "$question" "${mapred[@]}"; then
          "$question"
        else
          echo "Invalid question id" >&2
          echo "$usage" >&2
          exit 2
        fi
      fi
      exit
    ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      echo "$usage" >&2
      exit 2
    ;;
  esac
done

if [ -z "$1" ]; then
  setup 1>&2
  echo "setup() function executed" >&2
  echo "The answers generated by executing your solution are: " >&2

  for question in "${questions[@]}"; do
    echo "$question:"
    result="$("$question")"
    if [[ "$result" ]]; then
      echo "$result"
    else
      echo ""
    fi
  done
  cleanup 1>&2
  echo "cleanup() function executed" >&2
  echo "If you feel these values are correct please run:" >&2
  echo "./submitter" >&2
else
  echo "Invalid usage" >&2
  echo "$usage" >&2
  exit 2
fi
