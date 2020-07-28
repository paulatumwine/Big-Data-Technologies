package edu.miu.cs.cs523;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
        job.setReducerClass(AverageTemperatureReducer.class);

        job.setOutputKeyClass(PairWritable.class);
        job.setOutputValueClass(IntWritable.class);

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

    public static class AverageTemperatureMapper extends Mapper<LongWritable, Text, PairWritable, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String station = value.toString().substring(4, 10) + "-" + value.toString().substring(10, 15);
            Double temperature = Double.parseDouble(value.toString().substring(87, 92)) / 10;
            PairWritable pair = new PairWritable(station, temperature);

            Integer year = Integer.parseInt(value.toString().substring(15, 19));

            context.write(pair, new IntWritable(year));
        }
    }

    public static class AverageTemperatureReducer extends Reducer<PairWritable, IntWritable, PairWritable, IntWritable> {

        @Override
        public void reduce(PairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(key, val);
            }
        }
    }

    public static class PairWritable implements WritableComparable {
        private String station;
        private Double temp;

        public PairWritable() {
        }

        public PairWritable(String station, Double temp) {
            this.station = station;
            this.temp = temp;
        }

        public String getStation() {
            return station;
        }

        public void setStation(String station) {
            this.station = station;
        }

        public Double getTemp() {
            return temp;
        }

        public void setTemp(Double temp) {
            this.temp = temp;
        }

        @Override
        public int compareTo(Object o) {
            int result = ((PairWritable) o).getStation().compareTo(this.getStation());
            if (result != 0) return -1 * result; // reverse sort
            else return ((PairWritable) o).getTemp().compareTo(this.getTemp());
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(this.station);
            dataOutput.writeDouble(this.temp);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.station = dataInput.readUTF();
            this.temp = dataInput.readDouble();
        }

        @Override
        public String toString() {
            return this.station + "    " + this.temp;
        }
    }
}
