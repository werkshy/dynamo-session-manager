package net.energyhub.session;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodb.model.*;
import com.michelboudreau.alternator.AlternatorDB;
import com.michelboudreau.alternator.AlternatorDBClient;

import java.util.List;
import java.util.logging.Logger;

public class DynamoManagerTest {
    private static Logger log = Logger.getLogger("net.energyhub.session.DynamoManagerTest");

    private AlternatorDBClient client;
    private AlternatorDB db;
    private TestManager manager;
    private int maxInterval = 60;

    @Before
    public void setUp() throws Exception {
        this.client = new AlternatorDBClient();
        this.db = new AlternatorDB().start();
        this.manager = new TestManager(client);
        this.manager.setMaxInactiveInterval(maxInterval);
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

    @Test
    public void checkNextCurrentTableNames() {
        long startSeconds = (System.currentTimeMillis() / 1000) / maxInterval * maxInterval;

        String currentName = manager.getCurrentTableName(startSeconds);
        String previousName = manager.getPreviousTableName(startSeconds);
        assertEquals(currentName, manager.getCurrentTableName(startSeconds + 1));
        assertEquals(previousName, manager.getPreviousTableName(startSeconds + 1));
        assertFalse(currentName.equals(manager.getCurrentTableName(startSeconds + maxInterval)));
        assertFalse(previousName.equals(manager.getPreviousTableName(startSeconds + maxInterval)));
    }

    @Test
    public void ensureTableMakesWrite() {
        manager.ensureTable("test_table");

        GetItemRequest request = new GetItemRequest()
            .withTableName("test_table")
            .withKey(new Key().withHashKeyElement(new AttributeValue().withS("test_id")));
        // set eventual consistency or fully consistent
        request = request.withConsistentRead(true);

        GetItemResult result = client.getItem(request);
        assertNotNull(result);
        assertEquals("test_id", result.getItem().get("id").getS());
        assertEquals("test", result.getItem().get("data").getS());
    }


}
