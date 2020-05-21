package counters;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AssignmentReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    //reducer input : // Hydereabad    [ {39,3} {54.13} {9.5) {39,6}........ ]
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total=0;
        for(IntWritable count : values){
            total += count.get();
        }
        context.write(key,new IntWritable(total));
    }
}
