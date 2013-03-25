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

import com.amazonaws.AmazonClientException;
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
    protected static Integer maxInactiveInterval = 3600; // default in seconds
    protected static String ignoreUri = "";
    protected static String ignoreHeader = "";
    protected static Boolean logSessionContents = false;
    protected static Integer requestsPerSecond = 10; // for provisioning
    protected static Integer sessionSize = 1; // in kB
    protected static Boolean eventualConsistency = false;
    protected AmazonDynamoDB dynamo = null;
    protected DynamoTableRotator rotator = null;

    private DynamoSessionTrackerValve trackerValve;
    private ThreadLocal<StandardSession> currentSession = new ThreadLocal<StandardSession>();
    private Serializer serializer;

    //Either 'kryo' or 'java'
    private String serializationStrategyClass = "com.dawsonsystems.session.JavaSerializer";

    private Container container;

    private ScheduledExecutorService expiresExecutorService = null;
    private Pattern ignoreUriPattern = null;
    private Pattern ignoreHeaderPattern = null;

    /////////////////////////////////////////////////////////////////
    //   Getters and Setters for Implementation Properties
    /////////////////////////////////////////////////////////////////
    public static String getDynamoEndpoint() {
        return dynamoEndpoint;
    }

    public static void setDynamoEndpoint(String endpoint) {
        DynamoManager.dynamoEndpoint = endpoint;
    }

    public static String getTableBaseName() {
        return tableBaseName;
    }

    public static void setTableBaseName(String tableBaseName) {
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

    ////////////////////////////////////////////////////////////////////////////////
    //   Implement methods of Lifecycle
    ////////////////////////////////////////////////////////////////////////////////

    public void addLifecycleListener(LifecycleListener lifecycleListener) {
    }

    public LifecycleListener[] findLifecycleListeners() {
        return new LifecycleListener[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void removeLifecycleListener(LifecycleListener lifecycleListener) {
    }

    public void start() throws LifecycleException {
        log.info("Starting Dynamo Session Manager in container: " + this.getContainer().getName());
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
        log.info("Finished starting manager");
    }

    public void stop() throws LifecycleException {
        getDynamo().shutdown();
    }


    //////////////////////////////////////////////////////////////////////////////////
    // Implementation of Manager
    //////////////////////////////////////////////////////////////////////////////////

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
    public void removePropertyChangeListener(PropertyChangeListener propertyChangeListener) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void backgroundProcess() {
        if (rotator != null) {
            rotator.process();
        }
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

    public Session findSession(String id) throws IOException {
        return loadSession(id);
    }

    public Session loadSession(String id) throws IOException {
        if (rotator == null) {
            log.severe("Processing requests but rotator is not initialized");
        }
        if (rotator.getCurrentTableName() == null) {
            log.severe("No table is yet set to current");
        }

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
                    .withTableName(rotator.getCurrentTableName())
                    .withKey(new Key().withHashKeyElement(new AttributeValue().withS(id)));
            // set eventual consistency or fully consistent
            request = request.withConsistentRead(!eventualConsistency);

            GetItemResult result = getDynamo().getItem(request);

            // if not found in the current table, we look in the previous table
            if (result == null || result.getItem() == null && rotator.getPreviousTableName() != null) {
                log.fine("Falling back to previous table: " + rotator.getPreviousTableName());
                request = request.withTableName(rotator.getPreviousTableName());
                result = getDynamo().getItem(request);
            }

            if (result == null || result.getItem() == null) {
                log.info("Session " + id + " not found in Dynamo");
                StandardSession ret = (DynamoSession) createEmptySession();
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
            backgroundProcess(); // try to speed up processing
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

            PutItemRequest putRequest = new PutItemRequest().withTableName(rotator.getCurrentTableName()).withItem(dbData);
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
            DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(rotator.getCurrentTableName()).withKey(key);
            getDynamo().deleteItem(deleteItemRequest);
            if (rotator.getPreviousTableName() != null) {
                // TODO: this is something of an issue since we have provisioned the previous table to low-write-volume
                deleteItemRequest = deleteItemRequest.withTableName(rotator.getPreviousTableName());
                getDynamo().deleteItem(deleteItemRequest);
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Error removing session in Dynamo Session Store", e);
        } finally {
            currentSession.remove();
        }
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

    private void initDbConnection() throws LifecycleException {
        long nowSeconds = System.currentTimeMillis() / 1000;
        try {
            getDynamo();
            this.rotator = new DynamoTableRotator(getTableBaseName(), getMaxInactiveInterval(), getRequestsPerSecond(),
                    getSessionSize(), getEventualConsistency());
            rotator.setDynamo(getDynamo());
            String firstTable = rotator.createCurrentTableName(nowSeconds);
            log.info("initializing first table: " + firstTable);
            rotator.ensureTable(firstTable, DynamoTableRotator.CREATE_TABLE_HEADROOM_SECONDS * 2000);
            rotator.currentTableName = firstTable;

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
