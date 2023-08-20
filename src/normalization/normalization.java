package normalization;

import java.io.IOException;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class normalization {
    public static class normalizationMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\t");
            String[] names = lines[0].split(",");
            context.write(new Text(names[0]), new Text(names[1] + "," + lines[1]));
        }
    }

    public static class normalizationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            Map<String, Float> map = new HashMap<String, Float>();
            Float sum = 0.0f;
            for (Text line : value) {
                String[] names = line.toString().split(",");
                map.put(names[0], Float.parseFloat(names[1]));
                sum += Float.parseFloat(names[1]);
            }
            List<String> norm = new ArrayList<String>();
            for (String name : map.keySet()) {
                norm.add(name + "," + map.get(name) / sum);
            }
            // java7 don`t have String.join method, sad ...
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < norm.size(); i++) {
                sb.append(norm.get(i));
                if (i != norm.size() - 1) {
                    sb.append("|");
                }
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        /**
         * @brief configure the job
         */
        Configuration conf = new Configuration();

        /**
         * @brief job setup
         */
        Job job = Job.getInstance(conf, "normalization");
        job.setJarByClass(normalization.class);
        job.setMapperClass(normalizationMapper.class);
        job.setReducerClass(normalizationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        /**
         * @param args[0] role1,role2 num
         * @param args[1] outputDir
         */
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}