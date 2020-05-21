package flight;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text,Text,FlightWritable> {
    private FlightWritable flightInfo = new FlightWritable();
    public void map(LongWritable key, Text input, Context context) throws IOException, InterruptedException {
        //input = IX-2342,Kingfisher,Bangalore,Hyderabad,80,01-Jan-2017
        String[] values = input.toString().split(",");
        flightInfo.set(values[4],1,values[5],values[0]);
        String monthYear = flightInfo.getFlightDate().split("-")[1];
        monthYear += " " + flightInfo.getFlightDate().split("-")[2];        // key Jan 2017
        context.write(new Text(monthYear),flightInfo);
    }
}
