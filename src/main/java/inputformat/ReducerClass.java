package inputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text, Text,Text,IntWritable> {

    //reducer input : // Hydereabad    [ {39,3} {54.13} {9.5) {39,6}........ ]
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int totalRevanue = 0;
        for(Text data : values){
            String[] value = data.toString().split(",");
            totalRevanue+= Integer.parseInt(value[0])*Integer.parseInt(value[1]);
        }

        context.write(key,new IntWritable(totalRevanue));
    }
}
