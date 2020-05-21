package mapperchainning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperTwo extends Mapper<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        /*
         * Second Mapper reads in each word and convert all characters in it to lower case
         * It emits value as whatever the count of word read in (essentially ONE)
         */
        String lowerCaseWord = key.toString().toUpperCase();
        context.write(new Text(lowerCaseWord), value);
    }
}
