package multiinputclass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import wordcount.CombinerClass;

import java.io.IOException;


public class DriverClass {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration config = new Configuration();
        config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

        Path inputPath1 = new Path("hdfs://localhost:9000/multiinput1");

        Path inputPath2 = new Path("hdfs://localhost:9000/multiinput2");

        Path output_dir = new Path("hdfs://localhost:9000/multiinputOut");

        Job job = new Job(config,"Multiple-Input-Class");
        job.setJarByClass(DriverClass.class);
        job.setMapperClass(MapperClass1.class);
        job.setMapperClass(MapperClass2.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // adding the parameters for first input file
        MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class,MapperClass1.class);

        // adding the parameters for second input file
        MultipleInputs.addInputPath(job, inputPath2, KeyValueTextInputFormat.class,MapperClass2.class);

        FileOutputFormat.setOutputPath(job, output_dir);
        output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
