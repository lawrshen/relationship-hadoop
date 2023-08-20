package pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class PageRank {
    public static class PRInitMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            String name = tokens[0];
            String links = tokens[1];
            context.write(new Text(name), new Text("1\t" + links));
        }
    }

    public static class PRIterMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // value: <name>\t<pr>\t<links>
            // output: <name> + \t + "|" + <links>
            // output: <link name> + \t + <link pr>
            String[] tokens = value.toString().split("\t");
            String name = tokens[0];
            double pr = Double.parseDouble(tokens[1]);
            String[] links = tokens[2].split("\\|");
            for (String link : links) {
                String[] u = link.split(",");
                double weight = Double.parseDouble(u[1]);
                context.write(new Text(u[0]), new Text(String.valueOf(pr * weight)));
            }
            context.write(new Text(name), new Text("|" + tokens[2]));
        }
    }

    public static class PRIterReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double pr = 0;
            String links = "";
            for (Text value : values) {
                if (value.toString().startsWith("|")) {
                    links = value.toString().substring(1);
                } else {
                    pr += Double.parseDouble(value.toString());
                }
            }
            pr = 1.00 - 0.85 + 0.85 * pr;
            context.write(key, new Text(String.valueOf(pr) + "\t" + links));
        }
    }

    public static class PRViewerMapper extends Mapper<Object, Text, DoubleWritable, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            String name = tokens[0];
            String pr = tokens[1];
            String links = tokens[2];
            context.write(new DoubleWritable(Float.parseFloat(pr)), new Text(name + "\t" + links));
        }
    }

    private static class DoubleWritableDecreasingComparator extends DoubleWritable.Comparator {
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleWritable d1 = (DoubleWritable) w1;
            DoubleWritable d2 = (DoubleWritable) w2;
            return -1 * super.compare(d1, d2);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void movePath(Configuration conf, Path src, Path dst) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(dst)) {
            fs.delete(dst, true);
        }
        fs.rename(src, dst);
    }

    public static void init(Configuration conf, Path inputPath, Path outputPath) throws Exception {
        Job job = Job.getInstance(conf, "PageRank Init");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PRInitMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }

    public static void iterate(Configuration conf, Path inputPath, Path outputPath, int i) throws Exception {
        Job job = Job.getInstance(conf, "PageRank iter" + i);
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PRIterMapper.class);
        job.setReducerClass(PRIterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }

    public static void view(Configuration conf, Path inputPath, Path outputPath) throws Exception {
        Job job = Job.getInstance(conf, "PageRank viewer");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PRViewerMapper.class);
        job.setSortComparatorClass(DoubleWritableDecreasingComparator.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        // arg[0]: source path
        // arg[1]: output path
        // arg[2]: temp path
        Configuration conf = new Configuration();
        init(conf, new Path(args[0]), new Path(args[2]));
        for (int i = 0; i < 10; ++i) {
            iterate(conf, new Path(args[2]), new Path(args[1]), i + 1);
            movePath(conf, new Path(args[1]), new Path(args[2]));
        }
        view(conf, new Path(args[2]), new Path(args[1]));
        FileSystem.get(conf).delete(new Path(args[2]), true);
    }
}
