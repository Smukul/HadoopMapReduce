package bankloyalcustomer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        Path personInputFile = new Path("hdfs://localhost:9000/inputDir/person.txt");
        Path accountInputFile = new Path("hdfs://localhost:9000/inputDir/account.txt");
        Path outDirectory = new Path("hdfs://localhost:9000/bankLoyalCustomer");
        Job job = new Job(config,"BankLoyalCustomer");
        job.setJarByClass(Driver.class);
        job.setMapperClass(AccountMapper.class);
        job.setMapperClass(PersonMapper.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, accountInputFile,  TextInputFormat.class,  AccountMapper.class);
        MultipleInputs.addInputPath(job,  personInputFile, TextInputFormat.class, PersonMapper.class);

        FileOutputFormat.setOutputPath(job, outDirectory);
        outDirectory.getFileSystem(job.getConfiguration()).delete(outDirectory,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
