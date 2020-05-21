package evenoddsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


public class EvenODDDriverClass {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        Configuration config = new Configuration();

        String[] files=new GenericOptionsParser(config,args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        Job job = new Job(config,"EVENODDSUM");
        job.setJarByClass(EvenODDDriverClass.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        //job.setCombinerClass(CombinerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
