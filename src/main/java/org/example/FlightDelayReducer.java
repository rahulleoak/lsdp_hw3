package org.example;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
public class FlightDelayReducer extends Reducer<Text, Text, Text, DoubleWritable> {

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    double totalDelay = 0;
    int count = 0;

    List<Flight> f1Flights = new ArrayList<>();
    List<Flight> f2Flights = new ArrayList<>();

    for (Text value : values) {
      String[] parts = value.toString().split(",");
      String type = parts[0];
      String origin = parts[1];
      String destination = parts[2];
      int depTime = Integer.parseInt(parts[3]);
      int arrTime = Integer.parseInt(parts[4]);
      int delay = Integer.parseInt(parts[5]);

      if (type.equals("F1")) {
        f1Flights.add(new Flight(origin, destination, depTime, arrTime, delay));
      } else if (type.equals("F2")) {
        f2Flights.add(new Flight(origin, destination, depTime, arrTime, delay));
      }
    }

    // Match flights from F1 to F2 based on conditions
    for (Flight f1 : f1Flights) {
      for (Flight f2 : f2Flights) {
        if (f1.destination.equals(f2.origin) && f2.depTime > f1.arrTime) {
          totalDelay += f1.delay + f2.delay;
          count++;
        }
      }
    }

    if (count > 0) {
      double averageDelay = totalDelay / count;
      context.write(new Text("Average Delay"), new DoubleWritable(averageDelay));
    }
  }

}
