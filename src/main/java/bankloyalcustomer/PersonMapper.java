package bankloyalcustomer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PersonMapper extends Mapper<LongWritable, Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text input, Context context) throws IOException, InterruptedException {
        //input OMOI808692OZ,Allison,Abbott,21,female,Chicago
        String[] values = input.toString().split(",");
        context.write(new Text(values[0]),new Text("P,"+","+values[1]+","+values[2]));
    }
}
