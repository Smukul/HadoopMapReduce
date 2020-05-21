package evenoddsum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text,Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] numbers = line.split(",");
        for(String number : numbers){

            if(Integer.parseInt(number)%2 == 0){
                context.write(new Text("EVEN"),new IntWritable(Integer.parseInt(number)));
            } else {
                context.write(new Text("ODD"),new IntWritable(Integer.parseInt(number)));
            }
        }
    }
}
