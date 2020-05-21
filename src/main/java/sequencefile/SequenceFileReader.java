package sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileReader {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        Path path = new Path(args[0]);
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));

            Text key = new Text();
            //Text value = new Text();
            IntWritable value = new IntWritable();

            while (reader.next(key, value)) {
                System.out.println(key + ", " + value.toString());
            }
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}
