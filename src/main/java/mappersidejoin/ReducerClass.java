package mappersidejoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReducerClass extends Reducer<Text, IntWritable,Text,IntWritable> {

    //reducer input : //  STR_1 Bangalore   [ {280} {560} {456}.......]
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalRevanue = 0;
        for(IntWritable revanue : values){
            totalRevanue+= revanue.get();
        }

        context.write(key,new IntWritable(totalRevanue));
    }
}
