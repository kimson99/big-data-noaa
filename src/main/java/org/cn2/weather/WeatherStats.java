package org.cn2.weather;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WeatherStats implements Writable {

  private double maxTemp;
  private double minTemp;
  private double avgTemp;

  public WeatherStats() {}

  public WeatherStats(double max, double min, double avg) {
    this.maxTemp = max;
    this.minTemp = min;
    this.avgTemp = avg;
  }

  public void set(double max, double min, double avg) {
    this.maxTemp = max;
    this.minTemp = min;
    this.avgTemp = avg;
  }

  // 3. Serialization: How to write to disk
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(maxTemp);
    out.writeDouble(minTemp);
    out.writeDouble(avgTemp);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    maxTemp = in.readDouble();
    minTemp = in.readDouble();
    avgTemp = in.readDouble();
  }

  @Override
  public String toString() {
    return String.format("%.2f", maxTemp) + "\t" + String.format("%.2f", minTemp) + "\t" + String.format("%.2f", avgTemp);
  }
}
