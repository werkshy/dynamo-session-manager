package net.energyhub.session;

import static org.junit.Assert.*;

import com.amazonaws.services.dynamodb.model.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.michelboudreau.alternator.AlternatorDB;
import com.michelboudreau.alternator.AlternatorDBClient;

import java.lang.String;
import java.util.List;
import java.util.logging.Logger;

/**
 * User: oneill
 * Date: 3/22/13
 */
public class DynamoTableRotatorTest {
    private static Logger log = Logger.getLogger("net.energyhub.session.DynamoRotatorTest");

    private DynamoTableRotator rotator;
    private AmazonDynamoDB dynamo;
    private AlternatorDB db;
    private int maxInterval = 180;
    private int requestsPerSecond = 20;
    private int sessionSize = 2;
    private boolean eventualConsistency = false;

    @Before
    public void setUp() throws Exception {
        this.dynamo = new AlternatorDBClient();
        this.db = new AlternatorDB().start();
        this.rotator = new DynamoTableRotator("testTables", maxInterval, requestsPerSecond, sessionSize, eventualConsistency, dynamo);
    }

    @After
    public void tearDown() throws Exception {
        this.db.stop();
    }

    @Test
    public void testRotationRequired() throws Exception {
        long startSeconds = (System.currentTimeMillis() / 1000) / maxInterval*maxInterval;
        String staleName = rotator.createCurrentTableName(startSeconds -maxInterval - 1);
        rotator.currentTableName = staleName;

        // same time, table doesn't need to change
        assertFalse(rotator.rotationRequired(startSeconds -maxInterval - 1));
        // later, table does need to rotate
        assertTrue(rotator.rotationRequired(startSeconds + maxInterval + 1));

    }

    @Test
    public void testGetProvisionedThroughputObject() throws Exception {
        assertFalse(this.eventualConsistency);
        ProvisionedThroughput pt_rw = rotator.getProvisionedThroughputObject(false);
        long readsPerSession = (long) Math.ceil(Float.valueOf(sessionSize) /
                                                Float.valueOf(DynamoTableRotator.KBS_PER_READ_UNIT));
        long writesPerSession = (long) Math.ceil(Float.valueOf(sessionSize) /
                Float.valueOf(DynamoTableRotator.KBS_PER_WRITE_UNIT));
        assertEquals(readsPerSession * requestsPerSecond, pt_rw.getReadCapacityUnits().longValue());
        assertEquals(writesPerSession * requestsPerSecond, pt_rw.getWriteCapacityUnits().longValue());

        ProvisionedThroughput pt_ro = rotator.getProvisionedThroughputObject(true);
        assertEquals(readsPerSession * requestsPerSecond, pt_ro.getReadCapacityUnits().longValue());
        assertEquals(1L, pt_ro.getWriteCapacityUnits().longValue());
    }

    @Test
    public void testNextCurrentTableNames() {
        long startSeconds = (System.currentTimeMillis() / 1000) / maxInterval * maxInterval;

        String currentName = rotator.createCurrentTableName(startSeconds);
        String previousName = rotator.createPreviousTableName(startSeconds);
        assertEquals(currentName, rotator.createCurrentTableName(startSeconds + 1));
        assertEquals(previousName, rotator.createPreviousTableName(startSeconds + 1));
        assertFalse(currentName.equals(rotator.createCurrentTableName(startSeconds + maxInterval)));
        assertFalse(previousName.equals(rotator.createPreviousTableName(startSeconds + maxInterval)));
    }

    @Test
    public void testNextTableRequired() {
        long startSeconds = System.currentTimeMillis()/1000;
        long lastTableTime = startSeconds - startSeconds % maxInterval;
        String currentTableName = rotator.createCurrentTableName(startSeconds);
        rotator.currentTableName = currentTableName;

        assertFalse(rotator.createTableRequired(lastTableTime + 1));
        assertTrue(rotator.createTableRequired(lastTableTime + maxInterval - 1));

    }

    @Test
    public void rotation() {
        long startSeconds = 0;

        List<String> tables = dynamo.listTables().getTableNames();
        assertTrue(tables.isEmpty());

        // just starting, create current
        assertTrue(rotator.rotationRequired(startSeconds));
        rotator.rotateTables(startSeconds);
        tables = dynamo.listTables().getTableNames();
        assertEquals(1, tables.size());

        String firstTable = rotator.getCurrentTableName();
        assertNull(rotator.getPreviousTableName());

        // just current
        rotator.rotateTables(startSeconds + maxInterval  - 1);
        tables = dynamo.listTables().getTableNames();
        assertEquals(1, tables.size());
        assertEquals(firstTable, rotator.getCurrentTableName());

        // previous + current
        rotator.rotateTables(startSeconds + maxInterval + 1);
        tables = dynamo.listTables().getTableNames();
        assertEquals(2, tables.size());
        assertEquals(firstTable, rotator.getPreviousTableName());

        // previous + current
        rotator.rotateTables(startSeconds + 120);
        tables = dynamo.listTables().getTableNames();
        assertEquals(2, tables.size());
    }

    @Test
    public void createTable() throws Exception {
        String testTableName = "test_table_" + System.currentTimeMillis();
        rotator.ensureTable(testTableName, 10000);
        assertTrue(dynamo.listTables().getTableNames().contains(testTableName));
    }


    @Test
    public void ensureTableMakesWrite() throws Exception {
        String testTableName = "test_table_" + System.currentTimeMillis();
        rotator.ensureTable(testTableName, 10000);

        GetItemRequest request = new GetItemRequest()
                .withTableName(testTableName)
                .withKey(new Key().withHashKeyElement(new AttributeValue().withS("test_id")));
        // set eventual consistency or fully consistent
        request = request.withConsistentRead(true);

        GetItemResult result = dynamo.getItem(request);
        assertNotNull(result);
        assertEquals("test_id", result.getItem().get("id").getS());
        assertEquals("test", result.getItem().get("data").getS());
    }

    @Test
    public void isActive() throws Exception {
        // bit of a circular test!
        String testTableName = rotator.createCurrentTableName(System.currentTimeMillis()/1000);
        rotator.ensureTable(testTableName, 10000);
        assertTrue(rotator.isActive(testTableName));
    }

    @Test
    public void isWritable() throws Exception {
        // bit of a circular test!
        String testTableName = rotator.createCurrentTableName(System.currentTimeMillis()/1000);
        rotator.ensureTable(testTableName, 10000);
        assertTrue(rotator.isWritable(testTableName));
    }

    @Test
    public void init_previousExists() throws Exception {
        // Check that we pick up an active table that is from a previous time period
        long nowSeconds = System.currentTimeMillis()/1000;
        long twoTablesAgo = nowSeconds - 2*rotator.maxInactiveInterval;

        String oldTableName = rotator.createCurrentTableName(twoTablesAgo);
        rotator.ensureTable(oldTableName, 10000);

        rotator.init(nowSeconds);
        assertEquals(oldTableName, rotator.currentTableName);
    }

    @Test
    public void init_currentExists() throws Exception {
        // Check that we pick up an active table that is from the current time period
        long nowSeconds = System.currentTimeMillis()/1000;

        String tableName = rotator.createCurrentTableName(nowSeconds);
        rotator.ensureTable(tableName, 10000);

        rotator.init(nowSeconds);
        assertEquals(tableName, rotator.currentTableName);
    }

    @Test
    public void init_noneExists() throws Exception {
        // Check that we create and wait for a new table if none exists
        long nowSeconds = System.currentTimeMillis()/1000;

        String tableName = rotator.createCurrentTableName(nowSeconds);

        rotator.init(nowSeconds);
        assertEquals(tableName, rotator.currentTableName);
    }

    @Test
    public void isMyTable() {
        long nowSeconds = System.currentTimeMillis()/1000;

        String tableName = rotator.createCurrentTableName(nowSeconds);
        String fakeTableName = "some-other-base_" + rotator.dateFormat.format(nowSeconds);
        String almostTableName = rotator.tableBaseName + "-extra_" + rotator.dateFormat.format(nowSeconds);

        assertTrue(rotator.isMyTable(tableName));
        assertFalse(rotator.isMyTable(fakeTableName));
        assertFalse(rotator.isMyTable(almostTableName));

    }
}
