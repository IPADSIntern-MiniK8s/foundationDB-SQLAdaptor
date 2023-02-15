package org.sjtu.se.ipads.fdbserver.utils.counter;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.tuple.Tuple;
import org.sjtu.se.ipads.fdbserver.basicop.FdbTool;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// TODO: for currency, may need more check
public class Counter {
    static volatile int counter;
    Lock l = new ReentrantLock();

    public Counter(FdbTool fdbTool, Database db, String table_name) {
        l.lock();
        List<byte[]> result = fdbTool.queryAll(db, table_name);
        // the database is empty
        int size = result.size();
        if (size < 1) {
            counter = 1;
        } else {
            counter = Integer.parseInt(Tuple.fromBytes(result.get(size - 1)).getString(0));
        }
        l.unlock();
    }

    public void addCounter() {
        l.lock();
        counter += 1;
        l.unlock();
    }

    public int getCounter() {
        return counter;
    }
}
