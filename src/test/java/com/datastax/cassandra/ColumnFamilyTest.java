package com.datastax.cassandra;

import static org.junit.Assert.*;

import java.util.UUID;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import org.junit.Test;

public class ColumnFamilyTest extends BaseCassandraTest {
  
  @Test
  public void insertion() {
    Keyspace keyspace = cassandra.getKeyspace("Keyspace1");
    ColumnFamily columnFamily = keyspace.getColumnFamily("Standard1");
    Row row = new Row();
    row.put("column1", "value1");
    row.put(2, "value2");
    row.put(3, false);
    UUID id = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
    row.put(42, id);
    row.put(id, "uuid col name");
    
    
    columnFamily.insert(row);   
    
    CFCursor cursor = columnFamily.query(row);
    Row foundRow = cursor.next();
    assertEquals(row.getKey(), foundRow.getKey());
    assertEquals("value1",foundRow.getString("column1"));
    assertEquals("value2",foundRow.getString(2));
    assertEquals(false, foundRow.getBoolean(3));
    assertEquals(id,foundRow.getUUID(42));
    assertEquals("uuid col name", foundRow.getString(id));
  }
}
