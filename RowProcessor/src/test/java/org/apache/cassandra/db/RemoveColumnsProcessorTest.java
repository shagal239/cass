package org.apache.cassandra.db;

import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.proc.RemoveColumnsRowProcessor;
import org.apache.cassandra.io.SSTableReader;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;


public class RemoveColumnsProcessorTest {

    public static final String TABLE1 = "ks1";

    @Test
    public void test() throws IOException, ExecutionException, InterruptedException {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open(TABLE1);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);

        String key = "key1";
        RowMutation rm;

        RemoveColumnsRowProcessor.PERCENT = 0.8;
        RemoveColumnsRowProcessor.table = TABLE1;
        RemoveColumnsRowProcessor.MAXSIZE = 1000;

        // inserts
        rm = new RowMutation(TABLE1, key);
        for (int i = 0; i < 1000000; i++) {
            rm.add(new QueryPath(cfName, null, String.valueOf(i).getBytes()), new byte[0], 0);
        }
        rm.apply();

        cfs.forceBlockingFlush();


        // inserts
        rm = new RowMutation(TABLE1, key);
        for (int i = 0; i < 10; i++) {
            rm.add(new QueryPath(cfName, null, String.valueOf(i).getBytes()), new byte[0], 0);
        }
        rm.apply();

        cfs.forceBlockingFlush();


        // inserts
        rm = new RowMutation(TABLE1, key);
        for (int i = 0; i < 10; i++) {
            rm.add(new QueryPath(cfName, null, String.valueOf(i).getBytes()), new byte[0], 0);
        }
        rm.apply();
        cfs.forceBlockingFlush();


        CompactionManager.instance.doCompaction(cfs, Collections.singleton(cfs.getSSTables().iterator().next()), 0);


        CompactionManager.instance.doCompaction(cfs, Collections.singleton(cfs.getSSTables().iterator().next()), 0);


        Iterator<SSTableReader> iterator = cfs.getSSTables().iterator();
        CompactionManager.instance.doCompaction(cfs, Arrays.asList(iterator.next(), iterator.next()), 0);
        iterator = cfs.getSSTables().iterator();
        CompactionManager.instance.doCompaction(cfs, Arrays.asList(iterator.next(), iterator.next()), 0);


    }

}
