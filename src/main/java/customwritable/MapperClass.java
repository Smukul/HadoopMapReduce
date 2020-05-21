package customwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text,CustomWritable, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(",");
        for(String word : words){
            CustomWritable outputKey = new CustomWritable(word.toUpperCase().trim());
            IntWritable outputValue = new IntWritable(1);
            context.write(outputKey,outputValue);
        }
    }
}
