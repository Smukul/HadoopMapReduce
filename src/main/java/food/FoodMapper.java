package food;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FoodMapper extends Mapper<LongWritable, Text,Text,Text> {

    public void map(LongWritable key, Text input, Context context) throws IOException, InterruptedException {
        //input = JXJY167254JK,20-06-2017,2444454,Noodles:Pizza:Roti,97,Card,Sadabahar,09:31:21,3,Stale food
        String[] values = input.toString().split(",");
        context.write(new Text(values[0]),new Text(values[1]+","+values[8]+","+values[9]));
        // JXJY167254JK      20-06-2017,3,Stale food
    }
}
