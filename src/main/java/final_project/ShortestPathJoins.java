package final_project;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ShortestPathJoins extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(ShortestPathJoins.class);

    public static class HopMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] fields = value.toString().split("[\t,]");
            if (fields.length == 3) {
                final String origin = fields[0];
                final String dest = fields[1];
                final String distance = fields[2];
                context.write(new Text(origin), new Text(dest + "," + distance));
                context.write(new Text(dest), new Text(origin + "," + distance));
            }
        }
    }

    public static class HopReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            final Set<String> neighbors = new HashSet<>();
            for (final Text value : values) {
                neighbors.add(value.toString());
            }
            for (final String neighbor : neighbors) {
                context.write(key, new Text(neighbor));
            }
        }
    }

    public static class ShortestPathMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] fields = value.toString().split("[\t,]");
            if (fields.length == 2) {
                final String origin = fields[0];
                final String[] destDistance = fields[1].split(",");
                if (destDistance.length == 2) {
                    final String dest = destDistance[0];
                    final double distance = Double.parseDouble(destDistance[1]);
                    context.write(new Text(origin + "," + dest), new DoubleWritable(distance));
                }
            } else if (fields.length == 3) {
                final String origin = fields[0];
                final String dest = fields[1];
                final double distance = Double.parseDouble(fields[2]);
                context.write(new Text(origin + "," + dest), new DoubleWritable(distance));
            }
        }
    }

    public static class ShortestPathReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException {
            double minDistance = Double.MAX_VALUE;
            for (final DoubleWritable value : values) {
                minDistance = Math.min(minDistance, value.get());
            }
            context.write(key, new DoubleWritable(minDistance));
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        // Job 1: Perform 1 hop
        final Configuration conf1 = getConf();
        final Job job1 = Job.getInstance(conf1, "Shortest Path - 1 Hop");
        job1.setJarByClass(ShortestPathJoins.class);
        job1.setMapperClass(ShortestPathMapper.class);
        job1.setReducerClass(ShortestPathReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/hop1"));

        // Job 2: Perform 2 hops
        final Configuration conf2 = getConf();
        final Job job2 = Job.getInstance(conf2, "Shortest Path - 2 Hops");
        job2.setJarByClass(ShortestPathJoins.class);
        job2.setMapperClass(HopMapper.class);
        job2.setReducerClass(HopReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileInputFormat.addInputPath(job2, new Path(args[1] + "/hop1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/hop2"));

        // Job 3: Perform 3 hops
        final Configuration conf3 = getConf();
        final Job job3 = Job.getInstance(conf3, "Shortest Path - 3 Hops");
        job3.setJarByClass(ShortestPathJoins.class);
        job3.setMapperClass(HopMapper.class);
        job3.setReducerClass(HopReducer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileInputFormat.addInputPath(job3, new Path(args[1] + "/hop1"));
        FileInputFormat.addInputPath(job3, new Path(args[1] + "/hop2"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/hop3"));

        // Job 4: Find the shortest paths
        final Configuration conf4 = getConf();
        final Job job4 = Job.getInstance(conf4, "Shortest Path - Final");
        job4.setJarByClass(ShortestPathJoins.class);
        job4.setMapperClass(ShortestPathMapper.class);
        job4.setReducerClass(ShortestPathReducer.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(DoubleWritable.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job4, new Path(args[0]));
        FileInputFormat.addInputPath(job4, new Path(args[1] + "/hop1"));
        FileInputFormat.addInputPath(job4, new Path(args[1] + "/hop2"));
        FileInputFormat.addInputPath(job4, new Path(args[1] + "/hop3"));
        FileOutputFormat.setOutputPath(job4, new Path(args[2]));

        // Run all jobs sequentially
        int exitCode = job1.waitForCompletion(true) ? 0 : 1;
        if (exitCode == 0) {
            exitCode = job2.waitForCompletion(true) ? 0 : 1;
        }
        if (exitCode == 0) {
            exitCode = job3.waitForCompletion(true) ? 0 : 1;
        }
        if (exitCode == 0) {
            exitCode = job4.waitForCompletion(true) ? 0 : 1;
        }
        return exitCode;
    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Three arguments required:\n<input-dir> <intermediate-output-dir> <final-output-dir>");
        }

        try {
            ToolRunner.run(new ShortestPathJoins(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}