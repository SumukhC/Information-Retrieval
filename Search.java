/*
Author: Sumukh Chitrashekar
Email: schitras@uncc.edu
UNCC #: 801020249
*/


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Search extends Configured implements Tool {

    public static String USER_QUERY;
    public static final String DELIMITER = "#####";
    public static Set<String> QUERY_SET = new HashSet<>();

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Search(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job searchJob = Job.getInstance(getConf(), "Search");
        searchJob.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(searchJob, String.valueOf(new Path(args[0])));
        USER_QUERY = args[1]; //args[1] in quotes is the user query
        String[] userQuery = USER_QUERY.split(" ");
        for (int i = 0; i < userQuery.length; i++) {
            QUERY_SET.add(userQuery[i]);
        }
        String fileName = new String(USER_QUERY);
        String searchOutputDirectory = fileName.replaceAll(" ", "") + "OUTPUT";
        FileOutputFormat.setOutputPath(searchJob, new Path(searchOutputDirectory));
        searchJob.setMapperClass(SearchMapper.class);
        searchJob.setReducerClass(SearchReducer.class);
        searchJob.setOutputKeyClass(Text.class);
        searchJob.setOutputValueClass(DoubleWritable.class);
        return searchJob.waitForCompletion(true) ? 0 : 1;
    }

    public static class SearchMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(DELIMITER);
            String word = parts[0];
            String values = parts[1];
            String[] temp = values.split("\\t");
            String fileName = temp[0];
            Double val = Double.parseDouble(temp[1]);

            if (QUERY_SET.contains(word)) {
                context.write(new Text(fileName), new DoubleWritable(val));
            }
        }
    }

    public static class SearchReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double totalScore = 0;
            for (DoubleWritable doubleWritable : values) {
                totalScore += doubleWritable.get();
            }
            context.write(key, new DoubleWritable(totalScore));
        }
    }
}
