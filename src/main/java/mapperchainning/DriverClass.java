package mapperchainning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import wordcount.CombinerClass;

import java.io.IOException;


public class DriverClass {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration config = new Configuration();
        Path inputPath2 = new Path("hdfs://localhost:9000/inputDir/word-count.txt");
        Path output_dir = new Path("hdfs://localhost:9000/chainningOut");

        Job job = new Job(config,"ChainingMapper-1");
        job.setJarByClass(DriverClass.class);
        /* add multiple chained mappers to run for this job */
        ChainMapper.addMapper(job,
                MapperOne.class, /* Mapper to add to chain */
                LongWritable.class, /* Mapper input Key */
                Text.class, /* Mapper input value */
                Text.class, /* Mapper output key*/
                IntWritable.class, /* Mapper output value */
                config /* job configuration to use */
        );
        /* set second mapper in the chain */
        ChainMapper.addMapper(job,
                MapperTwo.class,
                Text.class,
                IntWritable.class,
                Text.class,
                IntWritable.class,
                config
        );
        /* set reducer for the chain */
        ChainReducer.setReducer(job,
                ReducerClass.class,
                Text.class,
                IntWritable.class,
                Text.class,
                IntWritable.class,
                config);

        FileInputFormat.addInputPath(job, inputPath2);
        FileOutputFormat.setOutputPath(job, output_dir);
        output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,true);
        /* run first MR */
        if (!job.waitForCompletion(true))
        {
            System.out.println("ERROR completing first job");
            System.exit(1);
        }
        /* MR-2 */
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "ChainingMapper-Job-2");
        Path output_dir2 = new Path("hdfs://localhost:9000/chainningOut_job2");
        job2.setJarByClass(DriverClass.class);
        job2.setMapperClass(JobTwoMapper.class);
        job2.setReducerClass(JobTwoReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        /* Set input path: Read from putput of first MR */
        FileInputFormat.addInputPath(job2, output_dir2);
        FileOutputFormat.setOutputPath(job2, output_dir2);
        //output_dir2.getFileSystem(job2.getConfiguration()).delete(output_dir2,true);
        /* run second MR */
        job2.waitForCompletion(true);
    }
}
