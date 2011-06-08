package com.datastax.cassandra;

import static org.junit.Assert.*;

import org.junit.Test;

public class ColumnFamilyTest extends BaseCassandraTest {

  @Test
  public void insertion() {
    Keyspace keyspace = cassandra.getKeyspace("Keyspace1");
    ColumnFamily columnFamily = keyspace.getColumnFamily("Standard1");
    Row row = new Row();
    row.put("column1", "value1");
    columnFamily.insert(row);   
    
    CFCursor cursor = columnFamily.query(row);
    Row foundRow = cursor.next();
    assertEquals(row.getKey(), foundRow.getKey());
  }
}
