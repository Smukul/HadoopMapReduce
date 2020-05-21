package multiinputclass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text, IntWritable,Text,IntWritable> {

    public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum =0;
        for(IntWritable value: values){
            sum+=value.get();
        }
        context.write(word, new IntWritable(sum));
    }
}
