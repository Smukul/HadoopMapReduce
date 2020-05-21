package flight;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        Path foodInputFile = new Path("hdfs://localhost:9000/inputDir/flight.txt");
        Path outDirectory = new Path("hdfs://localhost:9000/flightOut");
        Job job = new Job(config,"FlightAnalysis");
        job.setJarByClass(Driver.class);
        job.setNumReduceTasks(1);

        job.setMapperClass(FlightMapper.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlightWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, foodInputFile);
        FileOutputFormat.setOutputPath(job, outDirectory);
        outDirectory.getFileSystem(job.getConfiguration()).delete(outDirectory,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
