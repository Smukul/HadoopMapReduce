package join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class DriverClass {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration config = new Configuration();
        //config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

        Path empInfoPath = new Path("hdfs://localhost:9000/join1");
        Path addressPath = new Path("hdfs://localhost:9000/join2");

        Path outDirectory = new Path("hdfs://localhost:9000/joinOut");

        Job job = new Job(config,"MapReduce-Join");
        job.setJarByClass(DriverClass.class);
        job.setMapperClass(EmpMapper.class);
        job.setMapperClass(LocMapper.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // adding the parameters for first input file
        MultipleInputs.addInputPath(job, empInfoPath, TextInputFormat.class, EmpMapper.class);
        // adding the parameters for second input file
        MultipleInputs.addInputPath(job, addressPath, TextInputFormat.class, LocMapper.class);

        FileOutputFormat.setOutputPath(job, outDirectory);
        outDirectory.getFileSystem(job.getConfiguration()).delete(outDirectory,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
