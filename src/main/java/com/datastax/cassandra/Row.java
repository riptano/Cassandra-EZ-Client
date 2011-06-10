package com.datastax.cassandra;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
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
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

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
  
  public Row put(String columnName, String columnValue) {
    DynamicComposite dc = new DynamicComposite();
    dc.addComponent(columnName, StringSerializer.get());
    columnMap.put(StringSerializer.get().toByteBuffer(columnName), 
        new HColumnImpl<DynamicComposite, ByteBuffer>(dc, 
            StringSerializer.get().toByteBuffer(columnValue), 
            clockResolution.createClock(), dcs, ByteBufferSerializer.get()));
    return this;
  }
  
  public Row put(Object columnName, Object columnValue) {
    DynamicComposite dColName = new DynamicComposite();
    dColName.add(0,columnName);
    DynamicComposite dColValue = new DynamicComposite();
    dColValue.add(0, columnValue);
    columnMap.put(dColName.getComponent(0).getBytes(), 
        new HColumnImpl<DynamicComposite, ByteBuffer>(dColName, 
            dColValue.getComponent(0).getBytes(), 
            clockResolution.createClock(), dcs, ByteBufferSerializer.get()));
    return this;
  }
    
  
  void put(DynamicComposite columnName, ByteBuffer columnValue) {
    columnMap.put(columnName.getComponent(0).getBytes(), new HColumnImpl<DynamicComposite, ByteBuffer>(columnName, columnValue, 
        clockResolution.createClock(), dcs, ByteBufferSerializer.get()));
  }
  
  Map<ByteBuffer,HColumn<DynamicComposite, ByteBuffer>> getColumns() {
    return columnMap;
  }
  
  String getKey() {
    if ( key == null )
      key = TimeUUIDUtils.getTimeUUID(clockResolution).toString();
    return key;
  }
  
  public String getString(Object columnName) {
    DynamicComposite dc = new DynamicComposite();
    dc.add(0, columnName);
    return StringSerializer.get().fromByteBuffer(columnMap.get(dc.getComponent(0).getBytes()).getValue());
  }
  
  public int getInt(Object columnName) {
    DynamicComposite dc = new DynamicComposite();
    dc.add(0, columnName);
    return IntegerSerializer.get().fromByteBuffer(columnMap.get(dc.getComponent(0).getBytes()).getValue());
  }
  
  public long getLong(Object columnName) {
    DynamicComposite dc = new DynamicComposite();
    dc.add(0, columnName);
    return LongSerializer.get().fromByteBuffer(columnMap.get(dc.getComponent(0).getBytes()).getValue());
  }
  
  public Date getDate(Object columnName) {
    DynamicComposite dc = new DynamicComposite();
    dc.add(0, columnName);
    return DateSerializer.get().fromByteBuffer(columnMap.get(dc.getComponent(0).getBytes()).getValue());
  }
  
  public UUID getUUID(Object columnName) {
    DynamicComposite dc = new DynamicComposite();
    dc.add(0, columnName);
    return UUIDSerializer.get().fromByteBuffer(columnMap.get(dc.getComponent(0).getBytes()).getValue());
  }
  
  public boolean getBoolean(Object columnName) {
    DynamicComposite dc = new DynamicComposite();
    dc.add(0, columnName);
    return BooleanSerializer.get().fromByteBuffer(columnMap.get(dc.getComponent(0).getBytes()).getValue());
  }
  
  public byte[] getBytes(Object columnName) {
    DynamicComposite dc = new DynamicComposite();
    dc.add(0, columnName);
    return BytesArraySerializer.get().fromByteBuffer(columnMap.get(dc.getComponent(0).getBytes()).getValue());
  }
  
  
  
  ByteBuffer getKeyBytes() {
    return StringSerializer.get().toByteBuffer(getKey());
  }

}
