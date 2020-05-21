package inputformat;

import counters.AssigmentMapper;
import counters.AssignmentReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class DriverClass {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration config = new Configuration();
        //config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

        Path inputFile = new Path("hdfs://localhost:9000/inputDir/xml.txt");
        Path outDirectory = new Path("hdfs://localhost:9000/inputFormatOut");

        Job job = new Job(config,"CustomInputFormat");
        job.setJarByClass(DriverClass.class);
        job.setMapperClass(XMLMapper.class);
        //job.setReducerClass(AssignmentReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(XMLInputFormat.class);  //set your own input format class

        FileOutputFormat.setOutputPath(job, outDirectory);
        FileInputFormat.addInputPath(job,inputFile);
        outDirectory.getFileSystem(job.getConfiguration()).delete(outDirectory,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
