package org.cn2.trend.station;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StationYearMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text keyText = new Text();
    private DoubleWritable tempValue = new DoubleWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) {
        String line = value.toString();
        String[] parts = line.split(",");

        // Safety check for array bounds
        if (parts.length < 6)
            return;

        String id = parts[0];
        String date = parts[1];
        String element = parts[2];
        String dataValue = parts[3];
        String qFlag = parts[5];

        // Filter: Only TMIN/TMAX and empty Quality Flag
        if (!element.equals("TMIN") && !element.equals("TMAX")) {
            return;
        }
        if (!qFlag.isEmpty()) {
            return;
        }

        try {
            double temp = Double.POSITIVE_INFINITY;
            try {
                temp = Double.parseDouble(dataValue);
            } catch (NumberFormatException e) {
                return; // Skip invalid numbers
            }

            // Extract Year
            String year = date.substring(0, 4);

            // Composite Key: StationID#Year
            keyText.set(id + "#" + year);
            tempValue.set(temp);

            try {
                context.write(keyText, tempValue);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
