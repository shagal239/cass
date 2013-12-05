package org.apache.cassandra.db.proc;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;

import java.util.Iterator;
import java.util.Properties;


public class RemoveColumnsRowProcessor implements IRowProcessor {

    private static final double PERCENT = 0.8;
    private int gcBefore;

    public RemoveColumnsRowProcessor(int gcBefore)
    {
        this.gcBefore = gcBefore;
    }

    @Override
    public void setConfiguration(Properties config)
    {
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
        SecondaryIndexManager.Updater updater = SecondaryIndexManager.nullUpdater;
        int size = columns.getColumnCount();
        if(size > PERCENT * Integer.MAX_VALUE){
            Iterator<IColumn> iter = columns.iterator();
            for(int i = 0; i < size/4; i++){iter.next();}

            for(int i = 0; i < size/2; i++){
                IColumn column  = iter.next();
                iter.remove();
                updater.remove(column);
            }

        }
        return ColumnFamilyStore.removeDeleted(columns, gcBefore);
    }
}
