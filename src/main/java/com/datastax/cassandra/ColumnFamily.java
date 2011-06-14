package com.datastax.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.cassandra.model.MutatorImpl;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

public class ColumnFamily {

  private final ExecutingKeyspace keyspace;
  private final String columnFamilyName;
  private final ColumnFamilyTemplate<ByteBuffer,DynamicComposite> columnFamilyTemplate;
  
  private static final DynamicCompositeSerializer dcs = new DynamicCompositeSerializer();
  
  ColumnFamily(String columnFamilyName, ExecutingKeyspace keyspace) {
    this.columnFamilyName = columnFamilyName;
    this.keyspace = keyspace;    
    this.columnFamilyTemplate = 
      new ThriftColumnFamilyTemplate<ByteBuffer,DynamicComposite>(keyspace, 
          columnFamilyName, 
          ByteBufferSerializer.get(), 
          dcs);
  }
  
  public void insert(Row row) {

    for (Map.Entry<ByteBuffer,HColumn<DynamicComposite,ByteBuffer>> entry : row.getColumns().entrySet() ) {
      HColumn<DynamicComposite, ByteBuffer> hColumn = entry.getValue();
      columnFamilyTemplate.getMutator().addInsertion(row.getKeyBytes(), columnFamilyName, hColumn);
      // insert new row in index cf with key: cfname_colname and colname: Composite(value, rowkey, timestamp)
      // key: rowkey_colname and colname: timestamp colvalue: value
      // indexingService.index(Mutator, HColumn, rowKey)
    }
    columnFamilyTemplate.executeBatch();
  }
  
  public CFCursor query(Row row) {
    CFCursor cursor;
    if ( !row.hasColumns() ) {
      cursor = new CFCursor(columnFamilyTemplate.queryColumns(row.getKeyBytes()));
    } else {
      cursor = new CFCursor(columnFamilyTemplate.queryColumns(row.getKeyBytes(),row.getColumnsForQuery()));
    }
    return cursor;
  }
}
