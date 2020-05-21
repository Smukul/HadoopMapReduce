package join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LocMapper extends Mapper<LongWritable, Text,Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] addrInfo = value.toString().split(" ");
        context.write(new Text(addrInfo[0]),new Text("Address,"+addrInfo[1]));
    }
}
