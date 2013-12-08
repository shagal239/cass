package org.apache.cassandra.db;

import org.apache.cassandra.db.filter.IdentityQueryFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.proc.RemoveColumnsRowProcessor;
import org.apache.cassandra.io.SSTableReader;


import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static utils.Util.getRangeSlice;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import com.reardencommerce.kernel.collections.shared.evictable.*;
import org.junit.Test;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class RemoveColumnsProcessorTest {

    public static final String TABLE1 = "ks1";

    @Test
    public void test() throws IOException, ExecutionException, InterruptedException {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open(TABLE1);

        String cfName = "cf1";
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();
        String key = "test1";
        RowMutation rm;

        RemoveColumnsRowProcessor.PERCENT = 1;
        RemoveColumnsRowProcessor.table = TABLE1;
        RemoveColumnsRowProcessor.MAXSIZE = 25000;
        // inserts
        int initSize = (int) 1e7;
        rm = new RowMutation(TABLE1, key);
        for (int i = 0; i < initSize; i++) {

            rm.add(new QueryPath(cfName, null, String.valueOf(i).getBytes("UTF-8")), ByteBuffer.allocate(4).putInt(239).array(),  0);
            if(i % 1000 == 0){
            rm.apply();
                rm = new RowMutation(TABLE1, key);
                System.err.println(i);
            }
        }
        rm.apply();

        cfs.forceBlockingFlush();

        Iterator<SSTableReader> iterator = cfs.getSSTables().iterator();
        Collection<SSTableReader> sstables;
        sstables = cfs.getSSTables();

        CompactionManager.instance.doCompaction(cfs, sstables, CompactionManager.getDefaultGcBefore(cfs));

        ColumnFamily retrieved = cfs.getColumnFamily(new IdentityQueryFilter(key, new QueryPath(cfName)), Integer.MAX_VALUE);

        System.err.println(retrieved.getSortedColumns().size());

    }
}
