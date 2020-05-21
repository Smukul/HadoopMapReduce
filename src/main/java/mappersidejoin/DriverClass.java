package mappersidejoin;

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
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {
        Configuration config = new Configuration();
        //config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

        Path salesFile = new Path("hdfs://localhost:9000/mapperjoin/sales.txt");
        Path outDirectory = new Path("hdfs://localhost:9000/mapperjoinOut");
        //add files into distribute cache
        DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/mapperjoin/store.txt"),config);
        DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/mapperjoin/product.txt"),config);

        Job job = new Job(config,"MapperSide-Join");
        job.setJarByClass(DriverClass.class);
        job.setMapperClass(MapperJoin.class);
        job.setReducerClass(ReducerClass.class);
        //job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, outDirectory);
        FileInputFormat.addInputPath(job,salesFile);
        outDirectory.getFileSystem(job.getConfiguration()).delete(outDirectory,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
