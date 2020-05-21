package multiinputclass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass2 extends Mapper<Text, Text,Text, IntWritable> {

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value,new IntWritable(1));
    }
}
