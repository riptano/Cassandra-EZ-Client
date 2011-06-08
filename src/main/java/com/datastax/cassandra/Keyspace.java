package com.datastax.cassandra;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.cassandra.service.ThriftCluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

public class Keyspace {

  private final ExecutingKeyspace keyspace;  
  private final ThriftCluster thriftCluster;
  private final String keyspaceName;
  
  Keyspace(String keyspaceName, ThriftCluster cluster) {
    this.keyspaceName = keyspaceName;
    this.thriftCluster = cluster;
    keyspace = new ExecutingKeyspace(keyspaceName, thriftCluster.getConnectionManager(),
        HFactory.createDefaultConsistencyLevelPolicy(), FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE);  
  }
  
  public List<ColumnFamily> getColumnFamilies() {    
    KeyspaceDefinition ksDef = thriftCluster.describeKeyspace(keyspaceName);
    List<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>();
    for (ColumnFamilyDefinition cfd  : ksDef.getCfDefs()) {
      if ( !cfd.getName().equals("system") )
        columnFamilies.add(new ColumnFamily(cfd.getName(), keyspace));
    }
    return columnFamilies;
  }
  
  public ColumnFamily getColumnFamily(String columnFamilyName) {
    ColumnFamily columnFamily = new ColumnFamily(columnFamilyName, keyspace);
    
    return columnFamily;
  }
}
