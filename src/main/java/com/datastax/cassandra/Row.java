package com.datastax.cassandra;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.service.clock.MicrosecondsSyncClockResolution;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;

public class Row {
  
  private String key;
  private List<HColumn<String,String>> columns = new ArrayList<HColumn<String, String>>();
  private static MicrosecondsSyncClockResolution clockResolution = new MicrosecondsSyncClockResolution();
  
  public Row setKey(String key) {
    this.key = key;
    return this;
  }
  
  public Row put(String columnName, String columnValue) {
    columns.add(new HColumnImpl<String, String>(columnName, columnValue, HFactory.createClock()));
    return this;
  }
  
  List<HColumn<String,String>> getColumns() {
    return columns;
  }
  
  String getKey() {
    if ( key == null )
      key = TimeUUIDUtils.getTimeUUID(clockResolution).toString();
    return key;
  }

}
