package org.example;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.io.StringReader;

public class FlightDelayMapper extends Mapper<LongWritable, Text, Text, Text> {

  private static final String ORIGIN = "ORD";
  private static final String DESTINATION = "JFK";

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    // Configure CSVParser for flexible parsing options
    CSVParser csvParser = new CSVParserBuilder()
        .withSeparator(',')
        .withIgnoreQuotations(true)
        .build();

    // Use CSVReader with the configured parser
    try (CSVReader csvReader = new CSVReaderBuilder(new StringReader(value.toString()))
        .withCSVParser(csvParser)
        .build()) {

      String[] fields = csvReader.readNext();

      if (fields == null || fields.length < 49) {
        // Skip incomplete or null records
        return;
      }

      String year = fields[0];
      String month = fields[2];

      String origin = fields[11];               // Origin airport
      String destination = fields[17];          // Destination airport
      String flightDate = fields[5];            // Flight date
      String depTime = fields[24];              // Actual departure time
      String arrTime = fields[35];              // Actual arrival time
      String arrDelayMinutes = fields[37];      // Arrival delay in minutes
      String cancelled = fields[41];            // Cancelled indicator
      String diverted = fields[43];             // Diverted indicator

      // Apply date filter for June 2007 to May 2008
      int yearInt = Integer.parseInt(year);
      int monthInt = Integer.parseInt(month);
      if (!((yearInt == 2007 && monthInt >= 6) || (yearInt == 2008 && monthInt <= 5))) {
        return; // Skip flights outside the date range
      }


      // Check if the flight is cancelled or diverted
      if ("1".equals(cancelled) || "1".equals(diverted)) {
        return;
      }
      // Filter based on origin and destination conditions
      if (ORIGIN.equals(origin)) {
        // Emit flight originating from ORD
        String outputValue = destination + "," + depTime + "," + arrTime + "," + arrDelayMinutes;
        context.write(new Text(flightDate), new Text("F1," + outputValue));
      } else if (DESTINATION.equals(destination)) {
        // Emit flight arriving at JFK
        String outputValue = origin + "," + depTime + "," + arrTime + "," + arrDelayMinutes;
        context.write(new Text(flightDate), new Text("F2," + outputValue));
      }
    } catch (Exception e) {
      System.err.println("Error parsing record: " + e.getMessage());
    }
  }
}
