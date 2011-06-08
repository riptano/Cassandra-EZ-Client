package com.datastax.cassandra;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

public class KeyspaceTest extends BaseCassandraTest {
  
  @Test
  public void listColumnFamilies() {
    Keyspace keyspace = cassandra.getKeyspace("Keyspace1");
    List<ColumnFamily> columnFamilies = keyspace.getColumnFamilies();
    assertTrue(columnFamilies.size() > 1);
  }

}
