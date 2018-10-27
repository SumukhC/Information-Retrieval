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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.regex.Pattern;

public class TermFrequency extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(DocWordCount.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TermFrequency(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "termFrequency");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(job, String.valueOf(new Path(args[0])));	//args[0] is the input directory
        FileOutputFormat.setOutputPath(job, new Path(args[1]));	//args[1] is the output directory
        job.setMapperClass(TermFrequency.Map.class);
        job.setReducerClass(TermFrequency.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static DoubleWritable one = new DoubleWritable(1);
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String line = lineText.toString();
            Text currentWord;
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                }
                currentWord = new Text(word.toLowerCase() + "#####" + fileName);
                context.write(currentWord, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable count : counts) {
                sum += count.get();
            }
            double res = computeLogarithmic(sum);
            context.write(word, new DoubleWritable(res));
        }

		
		// Compute the log value
        private double computeLogarithmic(double sum) {
            if (sum > 0) {
                return (1 + Math.log10(sum));
            } else {
                return 0;
            }
        }
    }
}
