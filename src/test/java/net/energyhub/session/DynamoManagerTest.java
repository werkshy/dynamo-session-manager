package net.energyhub.session;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.michelboudreau.alternator.AlternatorDB;
import com.michelboudreau.alternator.AlternatorDBClient;
import java.util.logging.Logger;

public class DynamoManagerTest {
    private static Logger log = Logger.getLogger("net.energyhub.session.DynamoManagerTest");

    private AlternatorDBClient client;
    private AlternatorDB db;
    private TestManager manager;

    @Before
    public void setUp() throws Exception {
        this.client = new AlternatorDBClient();
        this.db = new AlternatorDB().start();
        this.manager = new TestManager(client);
    }

    @After
    public void tearDown() throws Exception {
        db.stop();
    }

    @Test
    public void createTable() {
        manager.ensureTable("test_table");
        assertEquals("test_table", client.listTables().getTableNames().get(0));
    }
}
