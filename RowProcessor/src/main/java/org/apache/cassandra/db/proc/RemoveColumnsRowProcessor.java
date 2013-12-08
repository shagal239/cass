package org.apache.cassandra.db.proc;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


public class RemoveColumnsRowProcessor implements IRowProcessor {
    private static Logger logger = LoggerFactory.getLogger(RemoveColumnsRowProcessor.class);
    public static double PERCENT = 0.8;
    public static int MAXSIZE;
    public static boolean shouldProcessIncomplete = false, shouldProcessUnchanged = false, shouldProcessEmpty = false;
    public static String table;
    public ColumnFamilyStore columnFamilyStore;

    @Override
    public void setConfiguration(Properties config) {
        assert config.getProperty("class").equals("RemoveColumns") : config.getProperty("class") + " != " + "RemoveColumns";
        assert config.containsKey("table");
        assert config.containsKey("maxcolumncount");

        PERCENT = config.containsKey("percent") ? Double.parseDouble(config.getProperty("percent")) : PERCENT;
        table = config.getProperty("table");
        MAXSIZE = Integer.parseInt(config.getProperty("maxcolumncount"));
    }

    @Override
    public void setColumnFamilyStore(ColumnFamilyStore cfs) {
        columnFamilyStore = cfs;
    }

    @Override
    public boolean shouldProcessUnchanged() {
        return shouldProcessUnchanged;
    }


    @Override
    public boolean shouldProcessIncomplete() {
        return shouldProcessIncomplete;
    }


    @Override
    public boolean shouldProcessEmpty() {
        return shouldProcessEmpty;
    }

    @Override
    public ColumnFamily process(DecoratedKey key, ColumnFamily columns,
                                boolean incomplete) {

        int size = columns.getSortedColumns().size();

        if (size >= PERCENT * MAXSIZE) {
            logger.info("started removing columns, table {} key {} columnfamily {} size {}", table, key.key, columns.name(), size);
            Iterator<IColumn> columnIterator = columns.getSortedColumns().iterator();

            for (int i = 0; i < size / 4; i++) {
                columnIterator.next();
            }
            RowMutation rowMutation = new RowMutation(table, key.key);

            for (int i = 0; columnIterator.hasNext() && (i < (size / 2)); i++) {
                IColumn column = columnIterator.next();
                rowMutation.delete(new QueryPath(columns.name(), null, column.name()), (column.timestamp() + 1));
                if (i % 1000 == 0) {
                    try {
                        rowMutation.apply();
                        rowMutation = new RowMutation(table, key.key);
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                    }
                }
            }

            try {
                rowMutation.apply();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        return columnFamilyStore.getColumnFamily(new IdentityQueryFilter(key.key, new QueryPath(columns.name())), Integer.MAX_VALUE);
    }
}
