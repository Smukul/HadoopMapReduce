package inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class XMLRecordReader extends RecordReader<LongWritable, Text> {

    private final String startTag = "<MOVIES>";
    private final String endTag = "</MOVIES>";

    private LineReader lineReader;

    private long curr_position = 0;
    private long startofFile;
    private long endOfFile;

    private LongWritable key = new LongWritable();
    private Text value = new Text();

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration confgi = taskAttemptContext.getConfiguration();
        FileSplit fileSplit = (FileSplit)inputSplit;
        startofFile = fileSplit.getStart();
        endOfFile = startofFile  +fileSplit.getLength();
        FSDataInputStream inputStream = fileSplit.getPath().getFileSystem(confgi).open(fileSplit.getPath());
        inputStream.seek(startofFile);
        lineReader = new LineReader(inputStream,confgi);
        this.curr_position = startofFile;
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        /* clear any previous values */
        key.set(curr_position);
        value.clear();
        Text line = new Text();
        boolean startFound = false;
        while(curr_position < endOfFile){
            curr_position = curr_position+lineReader.readLine(line);
            if(!startFound && line.toString().equalsIgnoreCase(this.startTag)){
                /* start tag found and start reading further */
                startFound = true;
            } else if(startFound && line.toString().equalsIgnoreCase(this.endTag)){
                /* end tag found,  stop reading, remove last comma  */
                String withoutComma = value.toString().substring(0,  value.toString().length()-1);
                value.set(withoutComma);
                return true;
            } else if(startFound){
                /* read all data between start and end tag */
                /* remove xml tags from line */
                String content = line.toString().replaceAll("<[^>]+>", "");    // content = Titanic
                value.append(content.getBytes("utf-8"), 0, content.length());
                value.append(",".getBytes("utf-8"), 0, ",".length());
            }
        }
        return false;
    }

    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        /* return percentage of file read so far */
        return (curr_position - startofFile) / (float) (endOfFile - startofFile);
          //   37         - 0                       900 - 0    0.041
    }

    public void close() throws IOException {
        //close the line reader
        if(lineReader != null){
            lineReader.close();
        }
    }
}
