package mapperchainning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperOne extends Mapper<LongWritable, Text,Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /* First Mapper reads in each line, split it into words and emit every word */
        String line = value.toString();
        String[] words = line.split(",");
        for(String word : words){
            IntWritable outputValue = new IntWritable(1);
            context.write(new Text(word),outputValue);
        }
    }
}
