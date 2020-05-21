package customwritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWritable implements WritableComparable<CustomWritable> {
    private String word;

    public CustomWritable() {
        setWord("");
    }

    public CustomWritable(String word) {
        setWord(word);
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }


    public int compareTo(CustomWritable o) {
        return this.word.compareTo(o.word);
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput,this.word);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.word = WritableUtils.readString(dataInput);
    }

    @Override
    public String toString() {
        return "CustomWritable{" +
                "word='" + word + '\'' +
                '}';
    }
}
