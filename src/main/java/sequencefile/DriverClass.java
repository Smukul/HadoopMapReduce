package sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import wordcount.CombinerClass;

import java.io.IOException;


public class DriverClass {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration config = new Configuration();

        Path inputFile = new Path("hdfs://localhost:9000/evenodd");
        Path outDirectory = new Path("hdfs://localhost:9000/sequenceOut");
        Job job = new Job(config,"Sequence-File-Reader");
        job.setJarByClass(DriverClass.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /* set job to read in a sequence file instead of a text file */
        //job.setInputFormatClass(Text.class);
        /* OPTIMIZATIONS */
        /* Optimization-1: Work with binary sequence files for efficiency */
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        /* Optimization-2: Compress sequence file */
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outDirectory);
        outDirectory.getFileSystem(job.getConfiguration()).delete(outDirectory,true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
