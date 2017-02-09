import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Gowtham on 2/2/2017.
 */
public class statistics {
    public static class statMapper extends Mapper<Object, Text, Text, DoubleWritable>{
        private float summ = 0f, min = Float.MAX_VALUE, max = Float.MIN_VALUE, sumsq = 0f, count = 0f;
        private Text wordSum = new Text("sum");
        private Text wordMin = new Text("min");
        private Text wordMax = new Text("max");
        private Text squareSum = new Text("sumSq");
        private Text wordCount = new Text("count");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                double temp = Double.parseDouble(itr.nextToken());
                context.write(new Text("word"), new DoubleWritable(temp));
            }
        }
    }

    public static class statReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private double min = Double.MAX_VALUE, max = Double.MIN_VALUE, sum =0d, average, count = 0d, sqaureSum = 0d;
        private double stdev;
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
            for (DoubleWritable val: values
                 ) {
                double temp = val.get();
                count += 1;
                if (temp < min){
                    min = temp;
                }
                if (temp > max){
                    max = temp;
                }
                sum += temp;
                sqaureSum += temp * temp;
            }
            sqaureSum /= count;
            average = sum / count;
            System.out.println("min " + min + " max " + max + " average " + average + " Standard deviation " + stdev + " count " + count+ " square Sum: "+ sqaureSum);
            stdev = Math.sqrt(sqaureSum-Math.pow(average,2));
            System.out.println("Standard deviation: "+stdev);
            context.write(new Text("min"), new DoubleWritable(min));
            context.write(new Text("max"), new DoubleWritable(max));
            context.write(new Text("average"), new DoubleWritable(average));
            context.write(new Text("standard deviation"), new DoubleWritable(stdev));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "statistics");

        job.setJarByClass(statistics.class);
        job.setMapperClass(statMapper.class);
//        job.setCombinerClass(statReducer.class);
        job.setReducerClass(statReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)? 0 : 1);

    }
}
