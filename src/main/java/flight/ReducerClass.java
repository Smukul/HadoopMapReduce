package flight;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class ReducerClass extends Reducer<Text,FlightWritable,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<FlightWritable> input, Context context) throws IOException, InterruptedException {
        //input - Jan 2017  [ {05,1,01-Jan-2017,IX-2342} {93,1,01-Jan-2017,IX-4644} {186,1,01-Jan-2017,IX-8345} {10,1,02-Jan-2017,IX-2342}......]
        Map<String,Integer> tripCounts =  new HashMap<String, Integer>(); //flightNUmber, NumberOfTripsInThisMonth
        Map<String,Integer> passengerCount =  new HashMap<String, Integer>(); // flightNumber, TotalNumberOfPassengersForAllTripsIntHisMonth
        for(FlightWritable data : input){
            String flightNumber = data.getFlightNumber();               //IX-2342
            if(tripCounts.containsKey(flightNumber)){
                tripCounts.put(flightNumber,tripCounts.get(flightNumber)+1);
                passengerCount.put(flightNumber,passengerCount.get(flightNumber)+data.getPassengersCount());
            } else {
                tripCounts.put(flightNumber,1);
                passengerCount.put(flightNumber,data.getPassengersCount());
            }
        }
        // AveragePassengers, FlightNUmber
        Map<Double, String> treeMap = new TreeMap<Double, String>(new Comparator<Double>(){

            public int compare(Double o1, Double o2) {
                return o2.compareTo(o1);
            }
        });
        /* Calculate average passengers count for all flight for this month */
        String lessUtilizedFlitghts = "";
        for(Map.Entry<String,Integer> entry : passengerCount.entrySet()){
            String flightNumber = entry.getKey();      //  IX-2342
            int passengersCount = entry.getValue();      //  2859
            int tripCount = tripCounts.get(flightNumber);     // 27

            /* flights having more than 25 trips and less than 3000 passengers */
            if ((passengersCount < 3000) && (tripCount > 25))
                lessUtilizedFlitghts += "," + flightNumber;             // lessUtilizedFlitghts = IX-2342

            Double avgPassengersPerMonth = new Double(passengersCount/tripCount*1.0);
            treeMap.put(avgPassengersPerMonth, flightNumber);
        }
        String out = "";
        int count = 0;
        for (Map.Entry<Double, String> e : treeMap.entrySet())  {
            out += "," + e.getValue() + "(" + e.getKey() + ")";      //emit top three filghts for this month //
            if (count++ >= 2)
                break;
        }
        /* emit less utilized flights as well */
        out += " - " + lessUtilizedFlitghts;

        /* format: month, top_3_flights - lessUtilizedFlights */
        context.write(key, new Text(out));
    }
}
