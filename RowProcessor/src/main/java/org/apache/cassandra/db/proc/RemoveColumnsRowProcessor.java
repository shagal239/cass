package org.apache.cassandra.db.proc;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeoutException;


public class RemoveColumnsRowProcessor implements IRowProcessor {
    public static final int MAXROWMUTATIONS = 239;
    private Logger logger = LoggerFactory.getLogger(RemoveColumnsRowProcessor.class);
    private static final double PERCENT = 0.8;
    private int MAXSIZE;

    public String table;
    public int gcBefore;

    @Override
    public void setConfiguration(Properties config)
    {
        assert config.getProperty("class").equals("RemoveColumns") : config.getProperty("class") +" != "+"RemoveColumns";
        assert config.containsKey("table");
        assert config.containsKey("maxcolumncount");

        table = config.getProperty("table");
        MAXSIZE = Integer.parseInt(config.getProperty("maxcolumncount"));
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


        if (size > PERCENT * MAXSIZE) {
            logger.info("DADADADADADAD mi zashli v blok");
            Iterator<IColumn> columnIterator = columns.getSortedColumns().iterator();

            for (int i = 0; i < size / 4; i++) {
                columnIterator.next();
            }
            List<RowMutation> buffer = new ArrayList<RowMutation>();
            for (int i = 0; columnIterator.hasNext() && (i < size / 2); i++) {
                RowMutation rowMutation = new RowMutation(table, key.key);
                ColumnPath path = new ColumnPath(columns.name());
                IColumn column = columnIterator.next();
                path.setColumn(column.name());
                rowMutation.delete(new QueryPath(path), column.timestamp());
                buffer.add(rowMutation);
                if(buffer.size() >= MAXROWMUTATIONS){
                    try {
                        //logger.info("sending 100 RowMutations");
                        StorageProxy.mutateBlocking(buffer, ConsistencyLevel.ALL);
                        logger.info("we did it");
                    } catch (UnavailableException e) {
                        logger.error(e.getMessage());
                    } catch (TimeoutException e) {
                        logger.error(e.getMessage());
                    }
                    buffer.clear();
                        logger.info("cleaned buffer "  +buffer.size());
                }
            }
        }



        return columns;
    }
}
