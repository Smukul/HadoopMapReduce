package counters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class DriverClass {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration config = new Configuration();
        //config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

        //Path inputFile = new Path("hdfs://localhost:9000/counter/counter.txt");
        Path inputFile = new Path("hdfs://localhost:9000/counter/assignment-2.txt");
        Path outDirectory = new Path("hdfs://localhost:9000/counterOut");

        Job job = new Job(config,"CustomCounter");
        job.setJarByClass(DriverClass.class);
//        job.setMapperClass(CounterMapper.class);
//        job.setReducerClass(ReducerClass.class);
        job.setMapperClass(AssigmentMapper.class);
        job.setReducerClass(AssignmentReducer.class);
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, outDirectory);
        FileInputFormat.addInputPath(job,inputFile);
        outDirectory.getFileSystem(job.getConfiguration()).delete(outDirectory,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
