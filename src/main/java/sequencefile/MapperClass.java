package sequencefile;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, BytesWritable, Text, IntWritable> {

    public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        String value_str = new String(value.getBytes(),"UTF-8");
        String data[]= value_str.split(",");    //data =  [85 131 993 392 689....]

        for(String word : data){
            int number=Integer.parseInt(word.trim());
            if((number%2)==1){
                context.write(new Text("ODD"), new IntWritable(number));   //  ODD 85  131  993
            }
            else{
                context.write(new Text("EVEN"), new IntWritable(number));   //  EVEN  392
            }
        }
    }
}
