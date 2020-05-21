package successratefacebook;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text,Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] values = value.toString().split(",");
        context.write(new Text(values[3]),new Text(values[2]+","+values[4]+","+values[5]));

    }
}
