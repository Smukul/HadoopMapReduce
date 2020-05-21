package inputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class XMLMapper extends Mapper<LongWritable, Text,LongWritable, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //identity mapper-  reading and outputting as it is
        context.write(key, value);
    }
}
