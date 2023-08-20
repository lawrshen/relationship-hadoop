package coocurrence;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class coocurrence {
    public static class coocurrenceMapper extends Mapper<Object, Text, Text, IntWritable> {
        Set<String> tang = new HashSet<>(), sun = new HashSet<>(), zhu = new HashSet<>(), sha = new HashSet<>(),
                bai = new HashSet<>(), ru = new HashSet<>(), guan = new HashSet<>(), yu = new HashSet<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // preprocess the data like 猴王 -> 悟空
            String[] ts = { "唐僧", "唐三藏", "陈玄奘", "玄奘", "唐长老", "金蝉子", "旃檀功德佛", "江流儿", "江流" };
            for (String t : ts) {
                tang.add(t);
            }
            String[] wk = { "孙悟空", "悟空", "齐天大圣", "美猴王", "猴王", "斗战胜佛", "孙行者", "心猿", "金公" };
            for (String s : wk) {
                sun.add(s);
            }
            String[] bj = { "猪八戒", "猪悟能", "悟能", "八戒", "猪刚鬣", "老猪", "净坛使者", "天蓬元帅", "木母" };
            for (String z : bj) {
                zhu.add(z);
            }
            String[] ss = { "沙僧", "沙和尚", "沙悟净", "悟净", "金身罗汉", "卷帘大将", "刀圭" };
            for (String s : ss) {
                sha.add(s);
            }
            String[] lm = { "白龙马", "小白龙", "白马", "八部天龙马" };
            for (String s : lm) {
                bai.add(s);
            }
            String[] fz = { "如来佛祖", "如来" };
            for (String s : fz) {
                ru.add(s);
            }
            String[] ps = { "观音菩萨", "观音", "观世音菩萨", "观世音" };
            for (String s : ps) {
                guan.add(s);
            }
            String[] dd = { "玉帝", "玉皇大帝" };
            for (String s : dd) {
                yu.add(s);
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString().replaceAll("\\s+", " ");
            String[] names = str.split(" ");
            Set<String> filterName = new HashSet<>();
            for (int i = 0; i < names.length; i++) {
                if (tang.contains(names[i])) {
                    filterName.add("唐僧");
                } else if (sun.contains(names[i])) {
                    filterName.add("悟空");
                } else if (zhu.contains(names[i])) {
                    filterName.add("八戒");
                } else if (sha.contains(names[i])) {
                    filterName.add("沙僧");
                } else if (bai.contains(names[i])) {
                    filterName.add("白龙马");
                } else if (ru.contains(names[i])) {
                    filterName.add("如来");
                } else if (guan.contains(names[i])) {
                    filterName.add("观音");
                } else if (yu.contains(names[i])) {
                    filterName.add("玉帝");
                } else {
                    filterName.add(names[i]);
                }
            }
            String[] fn = new String[filterName.size()];
            filterName.toArray(fn);
            for (int i = 0; i < fn.length; i++) {
                for (int j = 0; j < fn.length; j++) {
                    if (i != j) {
                        context.write(new Text(fn[i] + "," + fn[j]), new IntWritable(1));
                    }
                }
            }
        }
    }

    public static class coocurrenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> value, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : value) {
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
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
        Job job = Job.getInstance(conf, "coocurrence");
        job.setJarByClass(coocurrence.class);
        job.setMapperClass(coocurrenceMapper.class);
        job.setReducerClass(coocurrenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /**
         * @param args[0] name spilt by space
         * @param args[1] outputDir
         */
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}