package com.datastax.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.BooleanSerializer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.clock.MicrosecondsSyncClockResolution;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

/**
 * Models an row in an Apache Cassandra column family. Allows for 
 * generic storage and retrieval of basic types while maintaining 
 * type safety.
 *  
 * @author zznate
 */
public class Row {
  
  private String key;
  private Map<ByteBuffer,HColumn<DynamicComposite, ByteBuffer>> columnMap = 
    new HashMap<ByteBuffer,HColumn<DynamicComposite, ByteBuffer>>();
  private static MicrosecondsSyncClockResolution clockResolution = new MicrosecondsSyncClockResolution();
  private static final DynamicCompositeSerializer dcs = new DynamicCompositeSerializer();
  
  public Row setKey(String key) {
    this.key = key;
    return this;
  }
   
  public Row put(Object columnName, Object columnValue) {
    DynamicComposite dColName = new DynamicComposite();
    dColName.add(0,columnName);
    if ( columnValue != null ) {
      DynamicComposite dColValue = new DynamicComposite();
      dColValue.add(0, columnValue);
      columnMap.put(dColName.getComponent(0).getBytes(), 
          new HColumnImpl<DynamicComposite, ByteBuffer>(dColName, 
              dColValue.getComponent(0).getBytes(), 
              clockResolution.createClock(), dcs, ByteBufferSerializer.get()));
    } else {
      columnMap.put(dColName.getComponent(0).getBytes(), 
          new HColumnImpl<DynamicComposite, ByteBuffer>(dColName, 
              ByteBuffer.wrap(new byte[0]), 
              clockResolution.createClock(), dcs, ByteBufferSerializer.get()));
    }
    return this;
  }
    
  public boolean hasColumns() {
    return !columnMap.isEmpty();
  }
   
  public String getString(Object columnName) {
    return get(StringSerializer.get(), columnName);
  }
  
  public int getInt(Object columnName) {
    return get(IntegerSerializer.get(), columnName);
  }
  
  public long getLong(Object columnName) {
    return get(LongSerializer.get(), columnName);
  }
  
  public Date getDate(Object columnName) {
    return get(DateSerializer.get(), columnName);
  }
  
  public UUID getUUID(Object columnName) {
    return get(UUIDSerializer.get(), columnName);
  }
  
  public boolean getBoolean(Object columnName) {
    return get(BooleanSerializer.get(), columnName);
  }
  
  public byte[] getBytes(Object columnName) {
    return get(BytesArraySerializer.get(), columnName);
  }
  
  /**
   * Generic method that preserves typing. Use this method with custom 
   * value serializers
   */
  public <T> T get(Serializer<T> serializer, Object columnName) {
    DynamicComposite dc = new DynamicComposite();
    dc.add(0, columnName);
    return serializer.fromByteBuffer(columnMap.get(dc.getComponent(0).getBytes()).getValue());
  }
  
  ByteBuffer getKeyBytes() {
    return StringSerializer.get().toByteBuffer(getKey());
  }
  Map<ByteBuffer,HColumn<DynamicComposite, ByteBuffer>> getColumns() {
    return columnMap;
  }
    
  List<DynamicComposite> getColumnsForQuery() {
    List<DynamicComposite> cols = new ArrayList<DynamicComposite>(columnMap.size());
    for (ByteBuffer buf : columnMap.keySet()) {
      cols.add(columnMap.get(buf).getName());
    }
    return cols;
  }
  
  String getKey() {
    if ( key == null )
      key = TimeUUIDUtils.getTimeUUID(clockResolution).toString();
    return key;
  }
  
  void put(DynamicComposite columnName, HColumn<DynamicComposite,ByteBuffer> column) {
    columnMap.put(columnName.getComponent(0).getBytes(), column);
  }
}
