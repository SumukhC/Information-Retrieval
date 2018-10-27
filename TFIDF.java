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
import java.util.ArrayList;
import java.util.regex.Pattern;

public class TFIDF extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(TFIDF.class);
    private static final String DELIMITER = "#####";
    private static int NUMBER_OF_FILES;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TFIDF(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job mapReduceJob1 = Job.getInstance(getConf(), "TFIDF-MR1");
        mapReduceJob1.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(mapReduceJob1, String.valueOf(new Path(args[0]))); //args[0] is the input directory
        NUMBER_OF_FILES = Integer.parseInt(args[2]); //args[2] is the number of files in input
        String mr1OutputDirectory = "MR1-Output";
        FileOutputFormat.setOutputPath(mapReduceJob1, new Path(mr1OutputDirectory));
        mapReduceJob1.setMapperClass(MapTask1.class);
        mapReduceJob1.setReducerClass(ReduceTask1.class);
        mapReduceJob1.setOutputKeyClass(Text.class);
        mapReduceJob1.setOutputValueClass(DoubleWritable.class);
        boolean success = mapReduceJob1.waitForCompletion(true);

        if (success) {
            Job mapReduceJob2 = Job.getInstance(getConf(), "TFIDF-MR2");
            mapReduceJob2.setJarByClass(this.getClass());
            FileInputFormat.addInputPaths(mapReduceJob2, String.valueOf(new Path(mr1OutputDirectory + "/part-r-00000")));
            FileOutputFormat.setOutputPath(mapReduceJob2, new Path(args[1])); //args[1] is the output directory
            mapReduceJob2.setMapperClass(MapTask2.class);
            mapReduceJob2.setReducerClass(ReduceTask2.class);
            mapReduceJob2.setMapOutputKeyClass(Text.class);
            mapReduceJob2.setMapOutputKeyClass(Text.class);
            mapReduceJob2.setOutputKeyClass(Text.class);
            mapReduceJob2.setOutputValueClass(Text.class);
            success = mapReduceJob2.waitForCompletion(true);
        }
        return success? 0 : 1;
    }

	//Map Task 1
    public static class MapTask1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static DoubleWritable one = new DoubleWritable(1);
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        @Override
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String line = lineText.toString();
            Text currentWord;
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                }
                currentWord = new Text(word.toLowerCase() + DELIMITER + fileName);
                context.write(currentWord, one);
            }
        }
    }

	//Reduce Task 1
    public static class ReduceTask1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable count : counts) {
                sum += count.get();
            }
            double res = computeLogarithmic(sum);
            context.write(word, new DoubleWritable(res));
        }

        private double computeLogarithmic(double sum) {
            if (sum > 0) {
                return (1 + Math.log10(sum));
            } else {
                return 0;
            }
        }
    }

	//Map Task 2
    public static class MapTask2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(DELIMITER);
            Text outKey = new Text(parts[0]);
            String[] valueParts = parts[1].split("\\t");
            Text outValue = new Text(valueParts[0] + "=" + valueParts[1]);
            //MapWritable outValue = new MapWritable();
            //outValue.put(new Text(parts[1]), new DoubleWritable(val));
            context.write(outKey, outValue);
        }
    }

	//Reduce Task 2
    public static class ReduceTask2 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<Text> textArrayList = new ArrayList<>();
            int count = 0;
            for (Text value : values) {
                Text text = new Text(value.toString());
                textArrayList.add(text);
                ++count;
            }
            double tfidfScore = computeTFIDFScore(count);

            for (Text value : textArrayList) {
                String[] parts = value.toString().split("=");
                String fileName = parts[0];
                Text text = new Text(key.toString() + DELIMITER + fileName);
                double wf = Double.parseDouble(parts[1]);
                DoubleWritable result = new DoubleWritable(wf * tfidfScore);
                context.write(text, new Text(result.toString()));
            }

        }
		
		
		/*
		Compute the TFIDF score
		*/

        private double computeTFIDFScore(int count) {
            double idf = NUMBER_OF_FILES / count;
            return (Math.log10(1 + idf));
        }
    }
}
