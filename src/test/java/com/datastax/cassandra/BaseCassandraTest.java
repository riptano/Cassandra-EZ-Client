package com.datastax.cassandra;

import java.io.IOException;

import me.prettyprint.cassandra.testutils.EmbeddedServerHelper;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class BaseCassandraTest {
  private static EmbeddedServerHelper embedded;
  
  protected Cassandra cassandra;
  
  @Before
  public void localSetup() {
    if ( cassandra == null )
      cassandra = new Cassandra();
  }

  /**
   * Set embedded cassandra up and spawn it in a new thread.
   *
   * @throws TTransportException
   * @throws IOException
   * @throws InterruptedException
   */
  @BeforeClass
  public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException {
    if ( embedded == null ) {
      embedded = new EmbeddedServerHelper();
      embedded.setup();    
    }
  }


}
