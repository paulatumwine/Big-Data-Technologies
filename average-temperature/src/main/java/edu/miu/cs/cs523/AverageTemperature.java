package edu.miu.cs.cs523;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
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
        job.setCombinerClass(AverageTemperatureCombiner.class);
        job.setReducerClass(AverageTemperatureReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PairWritable.class);

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

    public static class AverageTemperatureMapper extends Mapper<LongWritable, Text, IntWritable, PairWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Integer year = Integer.parseInt(value.toString().substring(15, 19));
            Double temperature = Double.parseDouble(value.toString().substring(87, 92)) / 10;
            context.write(
                    new IntWritable(year),
                    new PairWritable(temperature, 1D)
            );
        }
    }

    public static class AverageTemperatureCombiner extends Reducer<IntWritable, PairWritable, IntWritable, PairWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0, count = 0;
            for (PairWritable val : values) {
                sum += val.getKey();
                count += val.getValue();
            }
            context.write(key, new PairWritable(sum, count));
        }
    }

    public static class AverageTemperatureReducer extends Reducer<IntWritable, PairWritable, IntWritable, DoubleWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0, count = 0, avg;
            for (PairWritable val : values) {
                sum += val.getKey();
                count += val.getValue();
            }
            avg = sum / count;
            context.write(key, new DoubleWritable(avg));
        }
    }

    public static class PairWritable implements Writable {
        private Double key;
        private Double value;

        public PairWritable() {
        }

        public PairWritable(Double key, Double value) {
            this.key = key;
            this.value = value;
        }

        public Double getKey() {
            return key;
        }

        public void setKey(Double key) {
            this.key = key;
        }

        public Double getValue() {
            return value;
        }

        public void setValue(Double value) {
            this.value = value;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeDouble(key);
            dataOutput.writeDouble(value);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            key = dataInput.readDouble();
            value = dataInput.readDouble();
        }

        @Override
        public String toString() {
            return "< " + key + " , " + value + " >";
        }
    }
}
