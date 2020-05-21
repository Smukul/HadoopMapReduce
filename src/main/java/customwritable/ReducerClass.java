package customwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReducerClass extends Reducer<CustomWritable, Text,CustomWritable,IntWritable> {


    public void reduce(CustomWritable word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum =0;
        for(IntWritable value: values){
            sum+=value.get();
        }
        context.write(word, new IntWritable(sum));
    }
}
