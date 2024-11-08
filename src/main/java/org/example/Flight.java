package org.example;

public class Flight {
  String origin;
  String destination;
  int depTime;
  int arrTime;
  int delay;

  // Updated constructor to include 'origin'
  Flight(String origin, String destination, int depTime, int arrTime, int delay) {
    this.origin = origin;
    this.destination = destination;
    this.depTime = depTime;
    this.arrTime = arrTime;
    this.delay = delay;
  }

  public String getOrigin() {
    return origin;
  }

  public void setOrigin(String origin) {
    this.origin = origin;
  }

  public String getDestination() {
    return destination;
  }

  public void setDestination(String destination) {
    this.destination = destination;
  }

  public int getDepTime() {
    return depTime;
  }

  public void setDepTime(int depTime) {
    this.depTime = depTime;
  }

  public int getArrTime() {
    return arrTime;
  }

  public void setArrTime(int arrTime) {
    this.arrTime = arrTime;
  }

  public int getDelay() {
    return delay;
  }

  public void setDelay(int delay) {
    this.delay = delay;
  }
}
