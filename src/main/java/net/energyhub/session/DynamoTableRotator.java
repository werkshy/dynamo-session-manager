package net.energyhub.session;

/***********************************************************************************************************************
 *
 * Dynamo Tomcat Sessions
 * ==========================================
 *
 * Copyright (C) 2013 by EnergyHub Inc. (http://www.energyhub.com)
 *
 ***********************************************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.model.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

/**
 * This class encapsulates the logic of rotating the tables and managing the current active and previous tables.
 *
 * Since we would need to do an expensive table scan to delete expired sessions from a single dynamo table,
 * we have taken the approach of rolling over to a new table, making the old table read-only, and migrating
 * active sessions to the new table.
 *
 * Sessions that are not active during the read-only time of the previous table are considered expired, and the
 * whole table is deleted upon the next rotation.
 *
 * This is called from Manager.backgroundProcess. I suggest setting Engine.backgroundProcessorDelay="1" in server.xml,
 * as this reduces the time during which different servers may be using different tables as the active table.
 *
 * Date: 3/22/13
 */
public class DynamoTableRotator {
    private static Logger log = Logger.getLogger("net.energyhub.session.DynamoTableRotator");
    public static final long CREATE_TABLE_HEADROOM_SECONDS = 60;
    public static final String TABLE_DATE_FORMAT = "yyyyMMdd_HHmmss";
    public static final short KBS_PER_READ_UNIT = 4;
    public static final short KBS_PER_WRITE_UNIT = 1;

    protected AmazonDynamoDB dynamo;
    protected String tableBaseName;
    protected Integer maxInactiveInterval;
    protected Integer requestsPerSecond; // for provisioning
    protected Integer sessionSize; // in kB
    protected Boolean eventualConsistency;
    protected String currentTableName;
    protected String previousTableName;
    protected Semaphore semaphore;

    protected SimpleDateFormat dateFormat = new SimpleDateFormat(TABLE_DATE_FORMAT);

    public DynamoTableRotator(String tableBaseName, Integer maxInactiveInterval, Integer requestsPerSecond,
                              Integer sessionSize, Boolean eventualConsistency, AmazonDynamoDB dynamo) {
        log.info("Initializing rotator");
        this.tableBaseName = tableBaseName;
        this.maxInactiveInterval = maxInactiveInterval;
        this.requestsPerSecond = requestsPerSecond;
        this.sessionSize = sessionSize;
        this.eventualConsistency = eventualConsistency;
        this.dynamo = dynamo;
        this.semaphore = new Semaphore(1);
        this.dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public synchronized String getCurrentTableName() {
        return this.currentTableName;
    }

    public synchronized String getPreviousTableName() {
        return this.previousTableName;
    }

    /**
     * During server startup, we may find that we need to create a new table (which will happen automatically in
     * the background.) Rather than waiting for the current table to use, let's go back in time and look for existing
     * active tables, and use that until the new one is online.
     * @param nowSeconds
     */
    public void init(long nowSeconds) throws InterruptedException {
        log.info("Initializing current table");

        for (int i=0; i<10; i++) {
            log.info("Searching for table from " + i*maxInactiveInterval + " seconds ago");
            long searchSeconds = nowSeconds - i*maxInactiveInterval;
            String tableName = createCurrentTableName(searchSeconds);
            if (isActive(tableName)) {
                // Triple-check the table works before using it
                ensureTable(tableName, DynamoTableRotator.CREATE_TABLE_HEADROOM_SECONDS*2000);
                synchronized (this) {
                    currentTableName = tableName;
                }
                log.info("Found and used active table " + tableName + " from " + i + " periods ago");
                return;
            }
        }
        // Hmmm, none found? Let's
        log.warning("No active tables found, will wait for the current one to come up and use that.");
        String firstTable = createCurrentTableName(nowSeconds);
        // If first table does not exist, go back and look for a previous table
        ensureTable(firstTable, DynamoTableRotator.CREATE_TABLE_HEADROOM_SECONDS * 2000);
        synchronized (this) {
            currentTableName = firstTable;
        }
    }

    /**
     * Process is called by the manager to initiate table management (or see if management is required.)
     * This is typically run during background processing. The null case requires
     *    - one check on time (next table change is < 60s away)
     *    - one check on current table vs time (is current table the one we're supposed to use?).
     */
    public void process() {
        if (dynamo == null) {
            log.severe("Can't manage table until dynamo is set");
            return;
        }

        boolean acquired = false;
        try {
            acquired = semaphore.tryAcquire();
            if (!acquired) {
                log.finer("Rotator is locked already, so this thread is not processing now.");
                return;
            }
            // Run table maintenance
            log.finer("Locked semaphore, checking table state");
            long nowSeconds = System.currentTimeMillis()/1000;
            if (createTableRequired(nowSeconds)) {
                log.info("Need to create next table");
                createTable(createNextTableName(nowSeconds));
            }

            if (rotationRequired(nowSeconds)) {
                log.info("Table rotation *is* required");
                rotateTables(nowSeconds);
            }

        } finally {
            if (acquired) {
                log.finer("Unlocking semaphore");
                semaphore.release();
            }
        }
    }

    protected boolean rotationRequired(long nowSeconds) {
        String targetCurrentTableName = createCurrentTableName(nowSeconds);
        return !targetCurrentTableName.equals(currentTableName);
    }

    /**
     * Look at existing tables to see if we need to pre-create the next new (future) table.
     */
    protected boolean createTableRequired(long nowSeconds) {
        long timeOfNextTable = nowSeconds + maxInactiveInterval - nowSeconds % maxInactiveInterval;
        if (timeOfNextTable >= nowSeconds + CREATE_TABLE_HEADROOM_SECONDS) {
            log.finer(timeOfNextTable-nowSeconds + " seconds until next table required, not doing it yet.");
            return false;
        }

        Set<String> tableNames = new HashSet<String>(dynamo.listTables().getTableNames());
        String nextTableName = createNextTableName(nowSeconds);
        if (!tableNames.contains(nextTableName)) {
            log.info(timeOfNextTable-nowSeconds + " seconds until next table required, we should create it.");
            return true;
        } else {
            log.finer("Next table is due but it already exists, not creating it");
        }
        return false;
    }

    protected void createTable(String tableName) {
        log.info("Creating table " + tableName);
        // define schema: primary string index on id
        KeySchemaElement primary = new KeySchemaElement().withAttributeName(DynamoManager.COLUMN_ID).withAttributeType(ScalarAttributeType.S);
        KeySchema schema = new KeySchema()
                .withHashKeyElement(primary);

        ProvisionedThroughput throughput = getProvisionedThroughputObject(false);

        CreateTableRequest createRequest = new CreateTableRequest(tableName, schema)
                .withProvisionedThroughput(throughput);
        dynamo.createTable(createRequest);
        // TODO: exception handling from create requests
        // either catch ResourceInUseException or superclass,
        // AmazonServiceException for creating existing table
    }

    protected void ensureTable(String tableName, long timeoutMillis) throws InterruptedException {
        List<String> tableNames = dynamo.listTables().getTableNames();
        if (!tableNames.contains(tableName)) {
            createTable(tableName);
        }
        waitForTable(tableName, timeoutMillis);
    }

    protected void waitForTable(String tableName, long timeoutMillis) throws InterruptedException {
        long waitStart = System.currentTimeMillis();

        while (true) {
            if (isActive(tableName)) {
                break;
            }
            if (System.currentTimeMillis() - waitStart > timeoutMillis) {
                log.severe("Timeout waiting for table " + tableName + " to become active");
                return;
            }
            Thread.sleep(1000);
        }

        while (true) {
            if (isWritable(tableName)) {
                break;
            }
            if (System.currentTimeMillis() - waitStart > timeoutMillis) {
                log.severe("Timeout waiting for table " + tableName + " to write/read");
                return;
            }

            Thread.sleep(100);
        }
    }


    /**
     * Return true if the current table exists and is ACTIVE
     * @param tableName
     * @return
     */
    protected boolean isActive(String tableName) {
        try {
            DescribeTableResult result = dynamo.describeTable(new DescribeTableRequest().withTableName(tableName));
            String status = result.getTable().getTableStatus();
            log.info("Table " + tableName + " state: " + status);
            return status.equals("ACTIVE");
        } catch (ResourceNotFoundException e) {
            log.info("Table " + tableName + " does not exist");
            return false;
        } catch (AmazonClientException e) {
            log.severe("Dynamo problems, assuming table " + tableName + " does not exist");
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Test table status by writing a test value and reading it back.
     * @param tableName
     * @return
     */
    protected boolean isWritable(String tableName) {
        String readBack = null;
        String testData = "test";
        String testId = "test_id";
        // sample write
        Map<String, AttributeValue> dbData = new HashMap<String, AttributeValue>();
        dbData.put(DynamoManager.COLUMN_ID, new AttributeValue().withS(testId));
        dbData.put(DynamoManager.COLUMN_DATA, new AttributeValue().withS(testData));
        dbData.put(DynamoManager.COLUMN_LAST_ACCESSED, new AttributeValue()
                .withN(Long.toString(System.currentTimeMillis(), 10)));

        PutItemRequest putRequest = new PutItemRequest()
                .withTableName(tableName)
                .withItem(dbData);
        try {
            dynamo.putItem(putRequest);
        } catch (AmazonClientException e) {
            log.info("Test put to " + tableName + " failed, wait and try again");
        }

        // sample read
        GetItemRequest request = new GetItemRequest()
                .withTableName(tableName)
                .withKey(new Key().withHashKeyElement(new AttributeValue().withS(testId)));
        request = request.withConsistentRead(true);

        GetItemResult getResult = null;
        try {
            getResult = dynamo.getItem(request);
        } catch (AmazonClientException e) {
            log.info("Test get from " + tableName + " failed, wait and try again");
        }
        if (getResult != null &&
                getResult.getItem() != null &&
                getResult.getItem().get(DynamoManager.COLUMN_DATA) != null) {
            readBack = getResult.getItem().get(DynamoManager.COLUMN_DATA).getS();
        }

        if (testData.equals(readBack)) {
            log.info("Successfully read back data from " + tableName);
            return true;
        }
        return false;

    }

    /**
     * Help calculate provisioned capacity
     *
     * @param kbPerUnit the kilobyte capacity of one unit (as of writing, this
     *                  is 4kb for reads, 1kb for writes)
     * @param requestsPerSecond expected request volume
     * @param sessionSize session size in kilobytes
     * @return how many strongly-consistent units should be provisioned
     */
    private long calculateUnitsRequired(short kbPerUnit, int requestsPerSecond, int sessionSize) {
        double unitsPerSessionRequest = Math.ceil((float) sessionSize / (float) kbPerUnit);
        return (long) (requestsPerSecond * unitsPerSessionRequest);
    }

    /**
     * Dynamically calculate the provisioned capacity for new or retiring tables
     * @param readOnly - used when a table is rotated into 'previous table' position.
     * @return
     */
    protected ProvisionedThroughput getProvisionedThroughputObject(boolean readOnly) {
        // TODO: bump up requestsPerSecond if we start seeing ProvisionedThroughputExceededExceptions

        long readUnit = calculateUnitsRequired(KBS_PER_READ_UNIT, requestsPerSecond, sessionSize);
        long writeUnit = calculateUnitsRequired(KBS_PER_WRITE_UNIT, requestsPerSecond, sessionSize);
        if (readOnly) {
            writeUnit = 1L;   // minimum is 1 unit, and we won't be writing to old tables.
        }
        // eventual consistency reads are two-for-the-price-of-one
        if (eventualConsistency) {
            readUnit = readUnit / 2;
        }
        ProvisionedThroughput throughput = new ProvisionedThroughput().withReadCapacityUnits(readUnit)
                .withWriteCapacityUnits(writeUnit);
        return throughput;
    }

    /**
     * Wait for the new current table to be writable, then set the new currentTable and previousTable names,
     * and provision the outgoing active table as read-only to save money.
     * @param nowSeconds
     */
    protected void rotateTables(long nowSeconds) {
        // Get some temp variables of what the current table *should* be called, but don't set the member field yet
        // until we know that the table actually exists
        String targetCurrentTableName = createCurrentTableName(nowSeconds);
        String targetPreviousTableName = currentTableName;

        List<String> tableNames = dynamo.listTables().getTableNames();

        // Make sure the table we want to make active exists
        try {
            ensureTable(targetCurrentTableName, CREATE_TABLE_HEADROOM_SECONDS*2000);
        } catch (Exception e) {
            log.severe("Failed to create table" + e);
            return;

        }
        synchronized (this) {
            log.info("Rotating current table from " + currentTableName + " to " + targetCurrentTableName);
            currentTableName = targetCurrentTableName;

            log.info("Rotating previous table from " + previousTableName + " to " + targetPreviousTableName);
            previousTableName = targetPreviousTableName;
        }

        removeExpiredTables(tableNames, nowSeconds);
    }

    /**
     * Check to see if we need to remove an expired table.
     * Removes any tables that aren't current, previous or next.
     */
    protected void removeExpiredTables(List<String> tableNames, long nowSeconds) {
        String nextTableName = createNextTableName(nowSeconds);
        Set<String> tablesToKeep = new HashSet<String>(Arrays.asList(currentTableName, previousTableName,
                nextTableName));
        for (String tableName : tableNames) {
            if (isMyTable(tableName) && !tablesToKeep.contains(tableName)) {
                try {
                    log.info("Deleting expired table: " + tableName);
                    DeleteTableRequest dtr = new DeleteTableRequest().withTableName(tableName);
                    dynamo.deleteTable(dtr);
                } catch (ResourceInUseException e) {
                    log.info("Table is already being deleted by another server/thread.");
                } catch (ResourceNotFoundException e) {
                    log.info("Table has already been deleted by another server/thread.");
                } catch (Exception e) {
                    log.severe("Failed to delete expired table " + tableName);
                    log.severe(e.toString());
                }
            }
        }
    }

    /**
     * Check that the basename matches the given tablename (and that we're not greedily grabbing a table with a
     * same-but-longer basename.
     * e.g. one cluster is using 'tomcat-sessions' and another using 'tomcat-sessions-beta'
     * @param tableName
     * @return
     */
    protected boolean isMyTable(String tableName) {
        if (!tableName.startsWith(tableBaseName)) {
            return false;
        }

        // Otherwise, let's make triple-sure that the table isn't base-table-SOMETHINGELSE-20130413...
        if (tableName.length() == tableBaseName.length() +  TABLE_DATE_FORMAT.length() + 1) {
            return true;
        }
        return false;
    }

    /**
     * Figure out the name of the current table using the current time.
     * We bin the sessions into tables every maxInactiveInterval seconds
     */
    protected String createCurrentTableName(long timestampSeconds) {
        long tableTimestamp = timestampSeconds - timestampSeconds % this.maxInactiveInterval;
        return tableBaseName + "_" + timestampSecondsToString(tableTimestamp);
    }

    /**
     * Figure out the name of the previous table using the current time.
     * We bin the sessions into tables every maxInactiveInterval seconds and
     * keep the last one around while we transition.
     */
    protected String createPreviousTableName(long timestampSeconds) {
        long tableTimestamp = timestampSeconds - timestampSeconds % this.maxInactiveInterval - this.maxInactiveInterval;
        return tableBaseName + "_" + timestampSecondsToString(tableTimestamp);
    }

    /**
     * Figure out the name of the next table using the current time.
     */
    protected String createNextTableName(long timestampSeconds) {
        long tableTimestamp = timestampSeconds - timestampSeconds % maxInactiveInterval + maxInactiveInterval;
        return tableBaseName + "_" + timestampSecondsToString(tableTimestamp);
    }

    /**
     * Format seconds-since-epoch into a string we can use in a dynamo table name
     * @param timestampSeconds
     * @return
     */
    protected String timestampSecondsToString(long timestampSeconds) {
        return this.dateFormat.format(new Date(timestampSeconds*1000));
    }
}
