package food;

import bankloyalcustomer.PersonMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        Path foodInputFile = new Path("hdfs://localhost:9000/inputDir/food.txt");
        Path outDirectory = new Path("hdfs://localhost:9000/foodOut");
        Job job = new Job(config,"FoodAnalysis");
        job.setJarByClass(Driver.class);
        job.setMapperClass(FoodMapper.class);
        job.setReducerClass(ReducerClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, foodInputFile);

        FileOutputFormat.setOutputPath(job, outDirectory);
        outDirectory.getFileSystem(job.getConfiguration()).delete(outDirectory,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
