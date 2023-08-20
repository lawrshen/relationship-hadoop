package preprocess;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.ansj.library.DicLibrary;

public class preprocess {
    public static class preprocessMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String nameFile = conf.get("nameList");
            Path namePath = new Path(nameFile);
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream in = fs.open(namePath);
            LineReader lineReader = new LineReader(in, conf);
            Text line = new Text();
            while (lineReader.readLine(line) > 0) {
                DicLibrary.insert(DicLibrary.DEFAULT, line.toString());
            }
            lineReader.close();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            Result result = DicAnalysis.parse(value.toString());
            for (Term t : result.getTerms()) {
                if (t.getNatureStr().equals("userDefine")) {
                    sb.append(t.getName() + " ");
                }
            }
            context.write(new Text(sb.toString()), new Text());
        }
    }

    public static class preprocessReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            context.write(key, new Text());
        }
    }

    public static void main(String[] args) throws Exception {
        /**
         * @brief configure the job
         */
        Configuration conf = new Configuration();
        conf.setStrings("nameList", args[2]);

        /**
         * @brief job setup
         */
        Job job = Job.getInstance(conf, "preprocess");
        job.setJarByClass(preprocess.class);
        job.setMapperClass(preprocessMapper.class);
        job.setReducerClass(preprocessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        /**
         * @param args[0] novels
         * @param args[1] outputDir
         * @param args[2] nameList
         */
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}