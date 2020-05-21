package flight;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightWritable implements Writable {
    private int passengersCount;
    private int tripsCount;
    private String flightDate;
    private String flightNumber;

    public int getPassengersCount() {
        return passengersCount;
    }

    public int getTripsCount() {
        return tripsCount;
    }

    public String getFlightDate() {
        return flightDate;
    }

    public String getFlightNumber() {
        return flightNumber;
    }

    public FlightWritable() {
        set("0", 0, "", "");
    }

    public void set(String passengersCount,int tripsCount,String dateStr,String flightNumber) {
        this.passengersCount = Integer.parseInt(passengersCount);
        this.flightDate = dateStr;
        this.tripsCount = tripsCount;
        this.flightNumber = flightNumber;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.passengersCount);
        dataOutput.writeInt(this.tripsCount);
        WritableUtils.writeString(dataOutput, this.flightDate);
        WritableUtils.writeString(dataOutput, this.flightNumber);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.passengersCount = dataInput.readInt();
        this.tripsCount = dataInput.readInt();
        this.flightDate = WritableUtils.readString(dataInput);
        this.flightNumber = WritableUtils.readString(dataInput);
    }
}
