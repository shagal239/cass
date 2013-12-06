package org.apache.cassandra.db.proc;

import org.apache.cassandra.db.*;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;


public class RemoveColumnsRowProcessor implements IRowProcessor {

    private static final double PERCENT = 0.8;

    public int gcBefore;

    @Override
    public void setConfiguration(Properties config)
    {
        assert config.getProperty("class").equals("RemoveColumns") : config.getProperty("class") +" != "+"RemoveColumns";
        for(Map.Entry entry : config.entrySet()){
            System.err.println(entry.getKey() + " " + entry.getValue());
        }
    }

    @Override
    public void setColumnFamilyStore(ColumnFamilyStore cfs)
    {
    }

    @Override
    public boolean shouldProcessIncomplete()
    {
        return false;
    }

    @Override
    public boolean shouldProcessUnchanged()
    {
        return false;
    }

    @Override
    public boolean shouldProcessEmpty()
    {
        return false;
    }

    @Override
    public ColumnFamily process(DecoratedKey key, ColumnFamily columns,
                                boolean incomplete)
    {

        System.err.println("yeah we are here and we are ready to start");
        System.err.println(key.key);
        System.err.println(incomplete);



        int size = columns.getSortedColumns().size();

        if (size > PERCENT * 1000) {

            System.err.println("size " + size);
            System.err.println("we are startinf");
            Iterator<IColumn> columnIterator = columns.getSortedColumns().iterator();
            for(int i = 0; i < size/4; i++){
                columnIterator.next();
            }
            for(int i = 0; i < size/2 && columnIterator.hasNext(); i++){
                Column column = (Column) columnIterator.next();
                columns.remove(column.name());

            }
            System.err.println("we are here");
            System.err.println("new size " + columns.getSortedColumns().size());
        }



        return columns;
    }
}
