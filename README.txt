Author: Sumukh Chitrashekar
Email: schitras@uncc.edu
UNCC #: 801020249


INSTRUCTIONS
============

1. There are 4 java files and each of them has to be executed to obtain results.
2. These programs were run in a Windows machine. And the program has executed successfully. If there are any issues while running in a Linux environment, please let me know. I shall bring my laptop and execute it in front of you.
3. The output files are in the same folder as this file, names accordingly.

To execute the programs, follow these steps:
============================================

1. Go to this link https://www.cloudera.com/documentation/other/tutorial/CDH5/topics/ht_usage.html where the instructions are there on how to execute a MapReduce program.
2. Follow the steps as given in the above link.
3. Each program has got different arguments that have to be supplied by user.

4. For DocWordCount.java, follow the rules as given in link. For step 5, the program accepts 2 arguments. args[0] is the input files directory, args[1] is the output file directory.
On executing the command in step 5 by giving input and output file directories, the output is produced in output directory mentioned. Step 6 of link shows how to display the output.

5. For TermFrequency.java, follow the rules as given in link. For step 5, the program accepts 2 arguments. args[0] is the input files directory, args[1] is the output file directory.
On executing the command in step 5 by giving input and output file directories, the output is produced in output directory mentioned. Step 6 of link shows how to display the output.

6. For TFIDF.java, follow the rules as given in link. For step 5, the program accepts 3 arguments. args[0] is the input files directory, args[1] is the output file directory. args[2] is the number of files.
On executing the command in step 5 by giving input and output file directories and file count (a number), the output is produced in output directory mentioned. Step 6 of link shows how to display the text. Remember the output directory of this program execution.

7. For Search.java, follow the rules as given in link. For step 5, the program accepts 2 arguments. args[0] is the input file PATH, which was the output args[1] and the result file named "part-r-00000" (example - /user/cloudera/wordcount/output/part-r-00000). args[2] is the search query in quotes.
On executing the command in step 5 by giving input and output file directories, the output is produced in output directory mentioned. Step 6 of link shows how to display the output.