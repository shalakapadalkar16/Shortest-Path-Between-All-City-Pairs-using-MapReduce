package final_project;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ShortestPathFloydWarshall extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(ShortestPathFloydWarshall.class);

    public static class ShortestPathMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private final Text pair = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] fields = value.toString().split(",");
            final String origin = fields[0];
            final String dest = fields[1];
            final double distance = Double.parseDouble(fields[2]);
            pair.set(origin + "," + dest);
            context.write(pair, new DoubleWritable(distance));
            pair.set(dest + "," + origin);
            context.write(pair, new DoubleWritable(distance));
        }
    }

    public static class ShortestPathReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException {
            double shortestDistance = Double.MAX_VALUE;
            for (final DoubleWritable val : values) {
                shortestDistance = Math.min(shortestDistance, val.get());
            }
            result.set(shortestDistance);
            context.write(key, result);
        }
    }

    public static class FloydWarshallMapper extends Mapper<Object, Text, IntWritable, Text> {
        private int NUM_PARTITIONS;
        @Override
        protected void setup(Context context) {
            NUM_PARTITIONS = context.getConfiguration().getInt("NUM_NODES", 10);
        }

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] fields = value.toString().split(",");
            final String source = fields[0];
            final String dest = fields[1];
            final double distance = Double.parseDouble(fields[2]);

            final int partitionId = getPartitionId(source);
            context.write(new IntWritable(partitionId), new Text(source + "," + dest + "," + distance));
        }

        private int getPartitionId(final String source) {
            return Math.abs(source.hashCode() % NUM_PARTITIONS);
        }
    }

    public static class FloydWarshallReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {
        @Override
        public void reduce(final IntWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            final Map<String, Map<String, Double>> distances = new HashMap<>();

            for (final Text value : values) {
                final String[] fields = value.toString().split(",");
                final String source = fields[0];
                final String dest = fields[1];
                final double distance = Double.parseDouble(fields[2]);

                Map<String, Double> destDistances = distances.computeIfAbsent(source, k -> new HashMap<>());
                destDistances.put(dest, distance);
            }

            final Set<String> vertices = distances.keySet();

            for (String intermediateVertex : vertices) {

                for (String sourceVertex : vertices) {

                    for (String destVertex : vertices) {
                        final Map<String, Double> sourceDistances = distances.get(sourceVertex);
                        final Map<String, Double> intermediateDistances = distances.get(intermediateVertex);

                        final double newDistance = getDistance(sourceDistances, intermediateVertex)
                                + getDistance(intermediateDistances, destVertex);

                        final double currentDistance = getDistance(sourceDistances, destVertex);
                        if (newDistance < currentDistance) {
                            sourceDistances.put(destVertex, newDistance);
                        }
                    }
                }
            }

            for (Map.Entry<String, Map<String, Double>> entry : distances.entrySet()) {
                final String sourceVertex = entry.getKey();
                final Map<String, Double> destDistances = entry.getValue();

                for (Map.Entry<String, Double> destEntry : destDistances.entrySet()) {
                    final String destVertex = destEntry.getKey();
                    final double distance = destEntry.getValue();
                    context.write(new Text(sourceVertex + "," + destVertex), new DoubleWritable(distance));
                }
            }
        }

        private double getDistance(final Map<String, Double> distances, final String vertex) {
            final Double distance = distances.get(vertex);
            return (distance != null) ? distance : Double.POSITIVE_INFINITY;
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        // Job 1: Find the shortest paths between directly connected nodes
        final Configuration conf1 = getConf();
        final Job job1 = Job.getInstance(conf1, "Shortest Path");
        int NUM_NODES = Integer.parseInt(args[3]);
        job1.setJarByClass(ShortestPathFloydWarshall.class);
        final Configuration jobConf1 = job1.getConfiguration();
        jobConf1.set("mapreduce.output.textoutputformat.separator", ",");
        job1.setMapperClass(ShortestPathFloydWarshall.ShortestPathMapper.class);
        job1.setReducerClass(ShortestPathFloydWarshall.ShortestPathReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        // Job 2: Find the shortest paths with up to 3 hops using Floyd-Warshall algorithm
        final Configuration conf2 = getConf();
        final Job job2 = Job.getInstance(conf2, "Floyd Warshall");
        job2.setJarByClass(ShortestPathFloydWarshall.class);
        final Configuration jobConf2 = job2.getConfiguration();
        jobConf2.set("mapreduce.output.textoutputformat.separator", ",");
        jobConf2.setInt("NUM_NODES", NUM_NODES);
        job2.setMapperClass(ShortestPathFloydWarshall.FloydWarshallMapper.class);
        job2.setReducerClass(ShortestPathFloydWarshall.FloydWarshallReducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // Run both jobs sequentially
        int exitCode = job1.waitForCompletion(true) ? 0 : 1;
        if (exitCode == 0) {
            exitCode = job2.waitForCompletion(true) ? 0 : 1;
        }
        return exitCode;
    }

    public static void main(final String[] args) {
        if (args.length != 4) {
            throw new Error("Four arguments required:\n<input-dir> <intermediate-output-dir> <final-output-dir> <num-nodes>");
        }

        try {
            ToolRunner.run(new ShortestPathFloydWarshall(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}