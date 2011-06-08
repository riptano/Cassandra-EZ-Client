package com.datastax.cassandra;

import java.util.Iterator;

import me.prettyprint.cassandra.service.template.ColumnFamilyResult;

public class CFCursor implements Iterator<Row> {

  private final ColumnFamilyResult<String, String> columnFamilyResult;
  
  CFCursor(ColumnFamilyResult<String, String> result) {
    this.columnFamilyResult = result;    
  }
  
  @Override
  public boolean hasNext() {
    return columnFamilyResult.hasNext();
  }

  @Override
  public Row next() {   
    // CFR is already at 1st position, JDBC style
    Row row = new Row();    
    row.setKey(columnFamilyResult.getKey());
    for (String columnName : columnFamilyResult.getColumnNames() ) {
      row.put(columnName, columnFamilyResult.getString(columnName));
    }
    if ( columnFamilyResult.hasNext() )
      columnFamilyResult.next();
    return row;
  }

  @Override
  public void remove() {
    columnFamilyResult.remove();    
  }
  
}
