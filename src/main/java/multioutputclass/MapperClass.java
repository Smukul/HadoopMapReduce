package multioutputclass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text,Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line[] = value.toString().split(",");
        //DJPX255251,Arthur,HR,6397,2016
        context.write(new Text(line[0]),new Text(line[1]+","+line[2]+","+line[3]));
    }
}
