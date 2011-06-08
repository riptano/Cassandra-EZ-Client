package com.datastax.cassandra;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCluster;

/**
 * Models the basics of a connection to Apache Cassandra
 *
 */
public class Cassandra {

  private final CassandraHostConfigurator cassandraHostConfigurator;
  private final ThriftCluster thriftCluster;
  
  public Cassandra() {
    this("localhost:9160",false);
  }
  
  public Cassandra(String hosts) {
    this(hosts,false);
  }
  
  public Cassandra(String hosts, boolean autoDiscover) {
    cassandraHostConfigurator = new CassandraHostConfigurator(hosts);
    cassandraHostConfigurator.setAutoDiscoverHosts(autoDiscover);
    if (autoDiscover)
      cassandraHostConfigurator.setRunAutoDiscoveryAtStartup(true);
    thriftCluster = new ThriftCluster("CassandraCluster", cassandraHostConfigurator);
  }
  
  
  public Keyspace getKeyspace(String keyspaceName) {
    Keyspace keyspace = new Keyspace(keyspaceName, thriftCluster);
    return keyspace;
  }
  
  public boolean connected() {
    return thriftCluster.describeClusterName() != null;
  }
}
