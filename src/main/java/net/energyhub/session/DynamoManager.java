/***********************************************************************************************************************
 *
 * Dynamo Tomcat Sessions
 * ==========================================
 *
 * Copyright (C) 2012-2013
 *   by Dawson Systems Ltd (http://www.dawsonsystems.com)
 *   and EnergyHub, Inc (http://www.energyhub.com)
 *
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

package net.energyhub.session;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.*;
import com.dawsonsystems.session.Serializer;
import org.apache.catalina.*;
import org.apache.catalina.connector.Request;
import org.apache.catalina.session.StandardSession;

import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class DynamoManager implements Manager, Lifecycle {
    private static Logger log = Logger.getLogger("net.energyhub.session.DynamoManager");
    protected static String awsAccessKey = "";  // Required for production environment
    protected static String awsSecretKey = "";  // Required for production environment
    protected static String dynamoEndpoint = ""; // used only for QA mock dynamo connections (not production)
    protected static String tableBaseName = "tomcat-sessions";
    protected static String currentTableName = "";
    protected static String previousTableName = "";
    protected static String ignoreUri = "";
    protected static String ignoreHeader = "";
    protected static Boolean logSessionContents = false;
    protected static Integer requestsPerSecond = 10; // for provisioning
    protected static Integer sessionSize = 1; // in kB
    protected static Boolean eventualConsistency = false;
    protected AmazonDynamoDB dynamo = null;

    private DynamoSessionTrackerValve trackerValve;
    private ThreadLocal<StandardSession> currentSession = new ThreadLocal<StandardSession>();
    private Serializer serializer;

    //Either 'kryo' or 'java'
    private String serializationStrategyClass = "com.dawsonsystems.session.JavaSerializer";

    private Container container;
    private int maxInactiveInterval;

    private ScheduledExecutorService expiresExecutorService = null;
    private Pattern ignoreUriPattern = null;
    private Pattern ignoreHeaderPattern = null;
    private static final long CREATE_TABLE_HEADROOM_SECONDS = 30;

    @Override
    public Container getContainer() {
        return container;
    }

    @Override
    public void setContainer(Container container) {
        this.container = container;
    }

    @Override
    public boolean getDistributable() {
        return false;
    }

    @Override
    public void setDistributable(boolean b) {

    }

    @Override
    public String getInfo() {
        return "Dynamo Session Manager";
    }

    @Override
    public int getMaxInactiveInterval() {
        return maxInactiveInterval;
    }

    @Override
    public void setMaxInactiveInterval(int i) {
        maxInactiveInterval = i;
    }

    @Override
    public int getSessionIdLength() {
        return 37;
    }

    @Override
    public void setSessionIdLength(int i) {

    }

    @Override
    public int getSessionCounter() {
        return 10000000;
    }

    @Override
    public void setSessionCounter(int i) {

    }

    @Override
    public int getMaxActive() {
        return 1000000;
    }

    @Override
    public void setMaxActive(int i) {

    }

    @Override
    public int getActiveSessions() {
        return 1000000;
    }

    @Override
    public int getExpiredSessions() {
        return 0;
    }

    @Override
    public void setExpiredSessions(int i) {

    }

    public int getRejectedSessions() {
        return 0;
    }

    public void setSerializationStrategyClass(String strategy) {
        this.serializationStrategyClass = strategy;
    }

    public void setRejectedSessions(int i) {
    }

    @Override
    public int getSessionMaxAliveTime() {
        return maxInactiveInterval;
    }

    @Override
    public void setSessionMaxAliveTime(int i) {

    }

    @Override
    public int getSessionAverageAliveTime() {
        return 0;
    }

    @Override
    public void setSessionAverageAliveTime(int i) {

    }
    public void load() throws ClassNotFoundException, IOException {
    }

    public void unload() throws IOException {
    }

    @Override
    public void backgroundProcess() {
        checkTableRotation(System.currentTimeMillis()/1000);
    }

    public void addLifecycleListener(LifecycleListener lifecycleListener) {
    }

    public LifecycleListener[] findLifecycleListeners() {
        return new LifecycleListener[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void removeLifecycleListener(LifecycleListener lifecycleListener) {
    }

    @Override
    public void add(Session session) {
        try {
            save(session);
        } catch (IOException ex) {
            log.log(Level.SEVERE, "Error adding new session", ex);
        }
    }

    @Override
    public void addPropertyChangeListener(PropertyChangeListener propertyChangeListener) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void changeSessionId(Session session) {
        session.setId(UUID.randomUUID().toString());
    }

    @Override
    public Session createEmptySession() {
        DynamoSession session = new DynamoSession(this);
        session.setId(UUID.randomUUID().toString());
        session.setMaxInactiveInterval(maxInactiveInterval);
        session.setValid(true);
        session.setCreationTime(System.currentTimeMillis());
        session.setNew(true);
        currentSession.set(session);
        log.fine("Created new empty session " + session.getIdInternal());
        return session;
    }

    /**
     * @deprecated
     */
    public org.apache.catalina.Session createSession() {
        return createEmptySession();
    }

    public org.apache.catalina.Session createSession(java.lang.String sessionId) {
        StandardSession session = (DynamoSession) createEmptySession();

        log.fine("Created session with requested id " + session.getIdInternal() + " ( " + sessionId + ")");
        if (sessionId != null) {
            session.setId(sessionId);
        }

        return session;
    }

    /**
     * There is no good way to return a list of sessions from dynamo without scanning the table.
     * It is a design goal of this project to avoid scanning the table, so this method just returns
     * an empty array.
     * @return
     */
    public org.apache.catalina.Session[] findSessions() {
        org.apache.catalina.Session[] sessions = new Session[]{};
        return sessions;
    }

    protected org.apache.catalina.session.StandardSession getNewSession() {
        log.fine("getNewSession()");
        return (DynamoSession) createEmptySession();
    }

    public void start() throws LifecycleException {
        for (Valve valve : getContainer().getPipeline().getValves()) {
            if (valve instanceof DynamoSessionTrackerValve) {
                trackerValve = (DynamoSessionTrackerValve) valve;
                trackerValve.setDynamoManager(this);
                log.info("Attached to Dynamo Tracker Valve");
                break;
            }
        }
        try {
            initSerializer();
        } catch (ClassNotFoundException e) {
            log.log(Level.SEVERE, "Unable to load serializer", e);
            throw new LifecycleException(e);
        } catch (InstantiationException e) {
            log.log(Level.SEVERE, "Unable to load serializer", e);
            throw new LifecycleException(e);
        } catch (IllegalAccessException e) {
            log.log(Level.SEVERE, "Unable to load serializer", e);
            throw new LifecycleException(e);
        }
        log.info("Will expire sessions after " + getMaxInactiveInterval() + " ms");
        initDbConnection();

        if (!getIgnoreUri().isEmpty()) {
            log.info("Setting URI ignore regex to: " + getIgnoreUri());
            this.ignoreUriPattern = Pattern.compile(getIgnoreUri());
        }
        if (!getIgnoreHeader().isEmpty()) {
            log.info("Setting header ignore regex to: " + getIgnoreHeader());
            this.ignoreHeaderPattern = Pattern.compile(getIgnoreHeader());
        }
    }

    public void stop() throws LifecycleException {
        getDynamo().shutdown();
    }

    public Session findSession(String id) throws IOException {
        return loadSession(id);
    }

    public static String getDynamoEndpoint() {
        return dynamoEndpoint;
    }

    public static void setDynamoEndpoint(String endpoint) {
        DynamoManager.dynamoEndpoint = endpoint;
    }

    public static String getTableBaseName() {
        return tableBaseName;
    }

    public static void setTableiBaseName(String tableBaseName) {
        DynamoManager.tableBaseName = tableBaseName;
    }

    public static String getAwsAccessKey() {
        return awsAccessKey;
    }

    public static void setAwsAccessKey(String awsAccessKey) {
        DynamoManager.awsAccessKey = awsAccessKey;
    }

    public static String getAwsSecretKey() {
        return awsSecretKey;
    }

    public static void setAwsSecretKey(String awsSecretKey) {
        DynamoManager.awsSecretKey = awsSecretKey;
    }


    public static String getIgnoreHeader() {
        return ignoreHeader;
    }

    public static void setIgnoreHeader(String ignoreHeader) {
        DynamoManager.ignoreHeader = ignoreHeader;
    }

    public static String getIgnoreUri() {
        return ignoreUri;
    }

    public static void setIgnoreUri(String ignoreUri) {
        DynamoManager.ignoreUri = ignoreUri;
    }

    public static Boolean getLogSessionContents() {
        return logSessionContents;
    }

    public static void setLogSessionContents(Boolean logSessionContents) {
        DynamoManager.logSessionContents = logSessionContents;
    }

    public static Integer getRequestsPerSecond() {
        return requestsPerSecond;
    }

    public static void setRequestsPerSecond(Integer requestsPerSecond) {
        DynamoManager.requestsPerSecond = requestsPerSecond;
    }

    public static Integer getSessionSize() {
        return sessionSize;
    }

    public static void setSessionSize(Integer sessionSize) {
        DynamoManager.sessionSize = sessionSize;
    }

    public static Boolean getEventualConsistency() {
        return eventualConsistency;
    }

    public static void setEventualConsistency(Boolean eventualConsistency) {
        DynamoManager.eventualConsistency = eventualConsistency;
    }

    public void clear() throws IOException {
        // TODO check result
        for (String tableName : getDynamo().listTables().getTableNames()) {
            if (tableName.startsWith(tableBaseName)) {
                log.info("Deleting table: " + tableName);
                getDynamo().deleteTable(new DeleteTableRequest(tableName));
            }
        }

        long nowSeconds = System.currentTimeMillis()/1000;
        ensureTable(getCurrentTableName(nowSeconds));
    }

    public void ensureTable(String tableName) {
        List<String> tableNames = getDynamo().listTables().getTableNames();
        if (tableNames.contains(tableName)) {
            // TODO verify schema
            log.info("Found existing table " + tableName);
            return;
        }
        log.info("Creating table " + tableName);
        // define schema: primary string index on id
        KeySchemaElement primary = new KeySchemaElement().withAttributeName("id").withAttributeType(ScalarAttributeType.S);
        KeySchema schema = new KeySchema()
                .withHashKeyElement(primary);

        ProvisionedThroughput throughput = getProvisionedThroughputObject(false);

        CreateTableRequest createRequest = new CreateTableRequest(tableName, schema)
                .withProvisionedThroughput(throughput);
        getDynamo().createTable(createRequest);
        // TODO: exception handling from create requests
        // either catch ResourceInUseException or superclass,
        // AmazonServiceException for creating existing table

        // waitForTableToBecomeAvailable
        String readBack = null;
        String testData = "test";
        String testId = "test_id";
        while (!testData.equals(readBack)) {
            // sample write
            Map<String, AttributeValue> dbData = new HashMap<String, AttributeValue>();
            dbData.put("id", new AttributeValue().withS(testId));
            dbData.put("data", new AttributeValue().withS(testData));
            dbData.put("lastmodified", new AttributeValue().withN(Long.toString(System.currentTimeMillis(), 10)));

            PutItemRequest putRequest = new PutItemRequest().withTableName(tableName).withItem(dbData);
            PutItemResult putResult = getDynamo().putItem(putRequest);

            // sample read
            GetItemRequest request = new GetItemRequest()
                .withTableName("test_table")
                .withKey(new Key().withHashKeyElement(new AttributeValue().withS(testId)));
            request = request.withConsistentRead(true);

            GetItemResult getResult = getDynamo().getItem(request);
            if (getResult != null) {
                readBack = getResult.getItem().get("data").getS();
            }
        }
    }

    /**
     * Dynamically calculate the provisioned capacity for new or retiring tables
     * @param readOnly - used when a table is rotated into 'previous table' position.
     * @return
     */
    protected ProvisionedThroughput getProvisionedThroughputObject(boolean readOnly) {
        long readUnit = requestsPerSecond*sessionSize;
        // by default, we need the same write throughput as read throughput (one read, one write per request).
        long writeUnit = readUnit;
        if (readOnly) {
            writeUnit = 0l;
        }
        // eventual consistency reads are two-for-the-price-of-one
        if (eventualConsistency) {
            readUnit = readUnit / 2;
        }
        ProvisionedThroughput throughput = new ProvisionedThroughput().withReadCapacityUnits(readUnit)
                .withWriteCapacityUnits(writeUnit);
        return throughput;
    }

    protected AmazonDynamoDB getDynamo() {
        if (this.dynamo != null) {
            return this.dynamo;
        }
        if (awsAccessKey.isEmpty() && dynamoEndpoint.isEmpty()) {
            log.severe("No connection properties specified for Dynamo");
            // FIXME try to connect anyway
            return null;
        }
        this.dynamo = new AmazonDynamoDBClient(new BasicAWSCredentials(awsAccessKey, awsSecretKey));
        log.info("Connecting to Dynamo with credentials: " + awsAccessKey + " / " + awsSecretKey);
        if (!dynamoEndpoint.isEmpty()) {
            // Using some sort of mock connection for QA/testing (see ddbmock or Alternator)
            log.info("Setting dynamo endpoint: " + dynamoEndpoint);
            this.dynamo.setEndpoint(dynamoEndpoint);
        }
        return this.dynamo;
    }

    public int getSize() throws IOException {
        QueryResult result = getDynamo().query(new QueryRequest().withTableName(currentTableName).withCount(true));
        return result.getCount();
    }

    public Session loadSession(String id) throws IOException {

        long t0 = System.currentTimeMillis();
        if (id == null || id.length() == 0) {
            return createEmptySession();
        }

        StandardSession session = currentSession.get();

        if (session != null) {
            if (id.equals(session.getId())) {
                return session;
            } else {
                currentSession.remove();
            }
        }
        try {
            log.fine("Loading session " + id + " from Dynamo");
            GetItemRequest request = new GetItemRequest()
                    .withTableName(currentTableName)
                    .withKey(new Key().withHashKeyElement(new AttributeValue().withS(id)));
            // set eventual consistency or fully consistent
            request = request.withConsistentRead(!eventualConsistency);

            GetItemResult result = getDynamo().getItem(request);

            // if not found in the current table, we look in the previous table
            if (result == null || result.getItem() == null && !previousTableName.isEmpty()) {
                log.fine("Falling back to previous table: " + previousTableName);
                request = request.withTableName(previousTableName);
                result = getDynamo().getItem(request);
            }

            if (result == null || result.getItem() == null) {
                log.info("Session " + id + " not found in Dynamo");
                StandardSession ret = getNewSession();
                ret.setId(id);
                currentSession.set(ret);
                return ret;
            }

            ByteBuffer data = result.getItem().get("data").getB();

            session = (DynamoSession) createEmptySession();
            session.setId(id);
            session.setManager(this);
            long t2 = System.currentTimeMillis();
            serializer.deserializeInto(data, session);
            long t3 = System.currentTimeMillis();
            log.fine("Deserialized session in " + (t3-t2) + "ms");

            session.setMaxInactiveInterval(-1);
            session.access();
            session.setValid(true);
            session.setNew(false);


            if (logSessionContents && log.isLoggable(Level.FINE)) {
                log.fine("Session Contents [" + session.getId() + "]:");
                for (Object name : Collections.list(session.getAttributeNames())) {
                    log.fine("  " + name);
                }
            }

            long t1 = System.currentTimeMillis();
            log.info("Loaded session id " + id + " in " + (t1-t0) + "ms, "
                    + result.getConsumedCapacityUnits() + " read units");
            currentSession.set(session);
            return session;
        } catch (IOException e) {
            log.severe(e.getMessage());
            throw e;
        } catch (ResourceNotFoundException e) {
            log.log(Level.SEVERE, "Unable to deserialize session ", e);
            log.info("Attempting to re-initialize table");
            ensureTable(currentTableName);
            throw e;
        } catch (ClassNotFoundException ex) {
            log.log(Level.SEVERE, "Unable to deserialize session ", ex);
            throw new IOException("Unable to deserializeInto session", ex);
        }
    }

    public void save(Session session) throws IOException {
        long t0 = System.currentTimeMillis();
        try {
            log.fine("Saving session " + session + " into Dynamo");

            StandardSession standardsession = (DynamoSession) session;

            if (logSessionContents && log.isLoggable(Level.FINE)) {
                log.fine("Session Contents [" + session.getId() + "]:");
                for (Object name : Collections.list(standardsession.getAttributeNames())) {
                    log.fine("  " + name);
                }
            }

            long t2 = System.currentTimeMillis();
            ByteBuffer data = serializer.serializeFrom(standardsession);
            long t3 = System.currentTimeMillis();
            log.fine("Serialized session in " + (t3-t2) + "ms");
            Map<String, AttributeValue> dbData = new HashMap<String, AttributeValue>();
            dbData.put("id", new AttributeValue().withS(standardsession.getIdInternal()));
            dbData.put("data", new AttributeValue().withB(data));
            dbData.put("lastmodified", new AttributeValue().withN(Long.toString(System.currentTimeMillis(), 10)));

            PutItemRequest putRequest = new PutItemRequest().withTableName(currentTableName).withItem(dbData);
            PutItemResult result = getDynamo().putItem(putRequest);

            long t1 = System.currentTimeMillis();
            log.fine("Updated session with id " + session.getIdInternal() + " in " + (t1 - t0) + "ms, "
                    + result.getConsumedCapacityUnits() + " write units.");
        } catch (IOException e) {
            log.severe(e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            currentSession.remove();
            log.fine("Session removed from ThreadLocal :" + session.getIdInternal());
        }
    }

    public void remove(Session session) {
        log.fine("Removing session ID : " + session.getId());
        Key key = new Key(new AttributeValue().withS(session.getId()));
        try {
            DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(currentTableName).withKey(key);
            getDynamo().deleteItem(deleteItemRequest);
            if (!previousTableName.isEmpty()) {
                deleteItemRequest = deleteItemRequest.withTableName(previousTableName);
                getDynamo().deleteItem(deleteItemRequest);
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Error removing session in Dynamo Session Store", e);
        } finally {
            currentSession.remove();
        }
    }

    @Override
    public void removePropertyChangeListener(PropertyChangeListener propertyChangeListener) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    // FIXME: this is being run from multiple threads?
    // the previous and current table names need to be static
    public static synchronized boolean rotateTables(Set<String> tableNames, String currentTableName_temp,
                                                    String previousTableName_temp) {
        boolean rotated = false;
        if (tableNames.contains(currentTableName_temp) && !currentTableName_temp.equals(currentTableName)) {
            log.info("Setting current table name to " + currentTableName_temp);
            currentTableName = currentTableName_temp;
        }

        // Check to see if we need to set the previous table name, and modify the table to be read only if we do.
        if (tableNames.contains(previousTableName_temp) && !previousTableName_temp.equals(previousTableName)) {
            log.info("Setting previous table name to " + previousTableName_temp);
            previousTableName = previousTableName_temp;
            rotated = true; // triggers the read-only modification on the previous table the first time it is moved.
        }
        return rotated;
    }

    /**
     * Check to see if we need to rotate the tables or delete an expired table.
     */
    public synchronized void checkTableRotation(long nowSeconds) {
        // check to see if we need to create a new table (future table)
        Set<String> tableNames = new HashSet(getDynamo().listTables().getTableNames());
        long timeOfNextTable = nowSeconds + getMaxInactiveInterval() - nowSeconds % getMaxInactiveInterval();
        String nextTableName = getNextTableName(nowSeconds);
        if (timeOfNextTable <= nowSeconds + CREATE_TABLE_HEADROOM_SECONDS && !tableNames.contains(nextTableName)) {
            log.info(timeOfNextTable-nowSeconds + " seconds until next table required, creating it now.");
            ensureTable(nextTableName);
        }

        // Get some temp variables of what the current table *should* be called, but don't set the member field yet
        // until we know that the table actually exists
        String currentTableName_temp = getCurrentTableName(nowSeconds);
        String previousTableName_temp = getPreviousTableName(nowSeconds);

        // check if we need to switch the currently active tableName (and previous table name)
        if (!tableNames.contains(currentTableName_temp)) {
            log.warning("The current Dynamo table doesn't exist, creating it now: " + currentTableName_temp);
            ensureTable(currentTableName_temp);
        }

        boolean rotated = rotateTables(tableNames, currentTableName_temp, previousTableName_temp);
        if (rotated) {
            // after rotation, the old table can be set to read-only to save $$$
            log.info("Reprovisioning the previous table to read-only: " + previousTableName);
            ProvisionedThroughput throughput = getProvisionedThroughputObject(true);
            UpdateTableRequest updateTableRequest = new UpdateTableRequest().withTableName(previousTableName)
                    .withProvisionedThroughput(throughput);
            getDynamo().updateTable(updateTableRequest);
        }

        // TODO: check to see if we need to remove an expired table
        // can probably do this asynchronously later
        Set<String> tablesToKeep = new HashSet<String>(Arrays.asList(currentTableName_temp, previousTableName_temp,
                nextTableName));
        Set<String> tablesToDelete = new HashSet<String>();
        for (String tableName : tableNames) {
            if (tableName.startsWith(tableBaseName) && !tablesToKeep.contains(tableName)) {
                log.info("Deleting expired table: " + tableName);
                DeleteTableRequest dtr = new DeleteTableRequest().withTableName(tableName);
                getDynamo().deleteTable(dtr);
            }
        }

    }

    /**
     * Figure out the name of the current table using the current time.
     * We bin the sessions into tables every maxInactiveInterval seconds
     *
     */
    public String getCurrentTableName(long timestampSeconds) {
        long tableTimestamp = timestampSeconds - timestampSeconds % getMaxInactiveInterval();
        return tableBaseName + "_" + tableTimestamp;
    }

    /**
     * Figure out the name of the previous table using the current time.
     * We bin the sessions into tables every maxInactiveInterval seconds and
     * keep the last one around while we transition.
     */
    public String getPreviousTableName(long timestampSeconds) {
        long tableTimestamp = timestampSeconds - timestampSeconds % getMaxInactiveInterval() - getMaxInactiveInterval();
        return tableBaseName + "_" + tableTimestamp;
    }

    /**
     * Figure out the name of the next table using the current time.
     */
    public String getNextTableName(long timestampSeconds) {
        long tableTimestamp = timestampSeconds - timestampSeconds % getMaxInactiveInterval() + getMaxInactiveInterval();
        return tableBaseName + "_" + tableTimestamp;
    }

    private void initDbConnection() throws LifecycleException {
        try {
            getDynamo();
            checkTableRotation(System.currentTimeMillis()/1000);

            log.info("Connected to Dynamo for session storage. Session live time = "
                    + (getMaxInactiveInterval()) + "s");
        } catch (Exception e) {
            e.printStackTrace();
            throw new LifecycleException("Error Connecting to Dynamo", e);
        }
    }

    private void initSerializer() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        log.info("Attempting to use serializer :" + serializationStrategyClass);
        serializer = (Serializer) Class.forName(serializationStrategyClass).newInstance();

        Loader loader = null;

        if (container != null) {
            loader = container.getLoader();
        }
        ClassLoader classLoader = null;

        if (loader != null) {
            classLoader = loader.getClassLoader();
        }
        serializer.setClassLoader(classLoader);
    }

    /**
     * Decide whether to skip loading/saving this request based on
     * ignoreUri and ignoreHeader regexes in configuration.
     */
    protected boolean isIgnorable(Request request) {
        if (this.ignoreUriPattern != null) {
            if (ignoreUriPattern.matcher(request.getRequestURI()).matches()) {
                log.fine("Session manager will ignore this session based on uri: " + request.getRequestURI());
                return true;
            }
        }
        if (this.ignoreHeaderPattern != null) {
            for (Enumeration headers = request.getHeaderNames(); headers.hasMoreElements(); ) {
                String header = headers.nextElement().toString();
                if (ignoreHeaderPattern.matcher(header).matches()) {
                    log.fine("Session manager will ignore this session based on header: " + header);
                    return true;
                }

            }
        }
        return false;
    }
}
