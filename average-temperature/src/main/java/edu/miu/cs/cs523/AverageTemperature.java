package edu.miu.cs.cs523;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class AverageTemperature extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        int res = ToolRunner.run(conf, new AverageTemperature(), args);

        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "edu.miu.cs.cs523.AverageTemperature");
        job.setJarByClass(AverageTemperature.class);

        job.setMapperClass(AverageTemperatureMapper.class);
        job.setReducerClass(AverageTemperatureReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem hdfs = FileSystem.get(getConf());

        Path inputDir = new Path(args[0]);
        FileInputFormat.addInputPath(job, inputDir);

        Path outputDir = new Path(args[1]);
        if (hdfs.exists(outputDir)) hdfs.delete(outputDir, true);
        FileOutputFormat.setOutputPath(job, outputDir);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class AverageTemperatureMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Integer year = Integer.parseInt(value.toString().substring(15, 19));
            Double temperature = Double.parseDouble(value.toString().substring(87, 92)) / 10;
            context.write(
                    new IntWritable(year),
                    new DoubleWritable(temperature)
            );
        }
    }

    public static class AverageTemperatureReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0, count = 0, avg;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            avg = sum / count;
            context.write(key, new DoubleWritable(avg));
        }
    }
}
