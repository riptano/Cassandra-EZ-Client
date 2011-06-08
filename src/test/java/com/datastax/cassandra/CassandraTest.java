package com.datastax.cassandra;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;


/**
 * test for Cassandra class
 */
public class CassandraTest extends BaseCassandraTest {
    
  
  @Test
  public void basicStartup() {   
    assertTrue(cassandra.connected());
  }
  
  @Test
  public void acquireKeyspace() {
    Keyspace keyspace = cassandra.getKeyspace("Keyspace1");
    assertNotNull(keyspace);
  }
  

}
