package lpa;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class LPA {
    public static class LPAInitMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            String[] links = tokens[1].split("\\|");
            for (String link : links) {
                String[] u = link.split(",");
                context.write(new Text(u[0]), new Text(tokens[0] + "," + u[1]));
            }
        }
    }

    public static class LPAInitReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append("|" + value.toString());
            }
            if (sb.length() != 0) {
                context.write(key, new Text(key.toString() + "\t" + sb.toString().substring(1)));
            }
        }
    }

    public static class LPAIterMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // value: <name>\t<label>\t<links>
            // output: <name> + ",a" + \t + <label> + \t + <links>
            // output: <link name> + ",b" + \t + <label> + "," + <link weight>
            String[] tokens = value.toString().split("\t");
            String[] links = tokens[2].split("\\|");
            for (String link : links) {
                String[] u = link.split(",");
                double weight = Double.parseDouble(u[1]);
                context.write(new Text(u[0] + ",b"), new Text(tokens[1] + "," + String.valueOf(weight)));
            }
            context.write(new Text(tokens[0] + ",a"), new Text(tokens[1] + "\t" + tokens[2]));
        }
    }

    public static class LPAIterPartitioner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = key.toString().split(",")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class LPAIterReducer extends Reducer<Text, Text, Text, Text> {
        private String name = "";
        private String oldLabel = "";
        private String links = "";

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String type = key.toString().split(",")[1];

            if (type.equals("a")) {
                name = key.toString().split(",")[0];
                String[] u = values.iterator().next().toString().split("\t");
                oldLabel = u[0];
                links = u[1];
            } else {
                HashMap<String, Double> mp = new HashMap<String, Double>();
                for (Text value : values) {
                    String[] u = value.toString().split(",");
                    if (mp.containsKey(u[0])) {
                        mp.put(u[0], mp.get(u[0]) + Double.parseDouble(u[1]));
                    } else {
                        mp.put(u[0], Double.parseDouble(u[1]));
                    }
                }
                double maxVal = 0.0;
                String newLabel = "";
                for (String k : mp.keySet()) {
                    double val = mp.get(k);
                    if (val > maxVal || (val == maxVal && !newLabel.equals(oldLabel))) {
                        maxVal = val;
                        newLabel = k;
                    }
                }
                context.write(new Text(name), new Text(newLabel + "\t" + links));
            }
        }
    }

    public static class LPAViewerMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            context.write(new Text(tokens[1]), new Text(tokens[0] + "\t" + tokens[2]));
        }
    }

    public static boolean compareLabel(Configuration conf, Path file1, Path file2) throws IOException {
        Scanner scanner1 = new Scanner(FileSystem.get(conf).open(file1));
        Scanner scanner2 = new Scanner(FileSystem.get(conf).open(file2));
        int distance = 0;
        while (scanner1.hasNextLine()) {
            if (!scanner2.hasNextLine()) {
                System.out.println("file1 has more lines than file2");
                System.exit(-1);
            }
            String[] tokens1 = scanner1.nextLine().split("\t");
            String[] tokens2 = scanner2.nextLine().split("\t");
            if (!tokens1[0].equals(tokens2[0])) {
                System.out.println("file1 and file2 have different keys");
                System.exit(-1);
            }
            distance += tokens1[1].equals(tokens2[1]) ? 0 : 1;
        }
        if (scanner2.hasNextLine()) {
            System.out.println("file2 has more lines than file1");
            System.exit(-1);
        }
        scanner1.close();
        scanner2.close();
        return distance == 0;
    }

    public static void movePath(Configuration conf, Path src, Path dst) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(dst)) {
            fs.delete(dst, true);
        }
        fs.rename(src, dst);
    }

    public static void init(Configuration conf, Path inputPath, Path outputPath) throws Exception {
        Job job = Job.getInstance(conf, "LPA Init");
        job.setJarByClass(LPA.class);
        job.setMapperClass(LPAInitMapper.class);
        job.setReducerClass(LPAInitReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }

    public static void iterate(Configuration conf, Path inputPath, Path outputPath, int i) throws Exception {
        Job job = Job.getInstance(conf, "LPA iter" + i);
        job.setJarByClass(LPA.class);
        job.setMapperClass(LPAIterMapper.class);
        job.setPartitionerClass(LPAIterPartitioner.class);
        job.setReducerClass(LPAIterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }

    public static void view(Configuration conf, Path inputPath, Path outputPath) throws Exception {
        Job job = Job.getInstance(conf, "LPA viewer");
        job.setJarByClass(LPA.class);
        job.setMapperClass(LPAViewerMapper.class);
        job.setOutputKeyClass(Text.class);
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
            if (compareLabel(conf, new Path(args[1] + "/part-r-00000"), new Path(args[2] + "/part-r-00000"))) {
                movePath(conf, new Path(args[1]), new Path(args[2]));
                break;
            }
            movePath(conf, new Path(args[1]), new Path(args[2]));
        }
        view(conf, new Path(args[2]), new Path(args[1]));
        FileSystem.get(conf).delete(new Path(args[2]), true);
    }
}
