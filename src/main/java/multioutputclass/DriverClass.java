package multioutputclass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class DriverClass {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration config = new Configuration();
        //config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

        Path inputPath = new Path("hdfs://localhost:9000/multioutput");

        Path output_dir = new Path("hdfs://localhost:9000/multioutputOut");

        Job job = new Job(config,"Multiple-Outputs");
        job.setJarByClass(DriverClass.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);  //add input path
        // Set 2 outputs
        MultipleOutputs.addNamedOutput(job,"HR", TextOutputFormat.class, Text.class, LongWritable.class );
        MultipleOutputs.addNamedOutput(job,"Accounts", TextOutputFormat.class, Text.class, LongWritable.class );

        FileOutputFormat.setOutputPath(job, output_dir);
        output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
