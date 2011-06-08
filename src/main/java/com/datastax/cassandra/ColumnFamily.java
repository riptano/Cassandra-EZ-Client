package com.datastax.cassandra;

import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.cassandra.model.MutatorImpl;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.beans.HColumn;

public class ColumnFamily {

  private final ExecutingKeyspace keyspace;
  private final String columnFamilyName;
  private final MutatorImpl<String> mutator;
  private final ColumnFamilyTemplate<String, String> columnFamilyTemplate;
  
  ColumnFamily(String columnFamilyName, ExecutingKeyspace keyspace) {
    this.columnFamilyName = columnFamilyName;
    this.keyspace = keyspace;
    this.mutator = new MutatorImpl<String>(keyspace);
    this.columnFamilyTemplate = 
      new ThriftColumnFamilyTemplate<String, String>(keyspace, 
          columnFamilyName, 
          StringSerializer.get(), 
          StringSerializer.get());
  }
  
  public void insert(Row row) {
    // for HColumn : row.columns
    for (HColumn<String, String> column : row.getColumns() ) {
      mutator.addInsertion(row.getKey(), columnFamilyName, column);
    }
    mutator.execute();
    mutator.discardPendingMutations();
  }
  
  public CFCursor query(Row row) {
    CFCursor cursor = new CFCursor(columnFamilyTemplate.queryColumns(row.getKey()));         
    return cursor;
  }
}
