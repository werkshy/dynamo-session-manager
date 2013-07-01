package net.energyhub.session;

import static org.junit.Assert.*;

import org.apache.catalina.Session;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodb.model.*;
import com.michelboudreau.alternator.AlternatorDB;
import com.michelboudreau.alternator.AlternatorDBClient;

import java.io.IOException;
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
        this.manager.start();
    }

    @After
    public void tearDown() throws Exception {
        this.manager.stop();
        db.stop();
    }

    @Test
    public void testSaveLoad() throws Exception {
        Session session = this.manager.createSession();
        log.info("Created session:" + session.getId() + "; access = " + session.getLastAccessedTime());

        this.manager.save(session);
        log.info("Saved session:" + session.getId() + "; access = " + session.getLastAccessedTime());
        String id = session.getId();
        long lastAccessed = session.getLastAccessedTime();

        Thread.sleep(1000l);

        session = this.manager.loadSession(id);
        log.info("Loaded session:" + session.getId() + "; access = " + session.getLastAccessedTime());
        // verify that loaded session still has the last accessed time we expect
        assertEquals(id, session.getId());
        assertEquals(lastAccessed, session.getLastAccessedTime());

        // Now save again and verify that the last accessed time has changed
        this.manager.save(session);
        log.info("Saved session (again):" + session.getId() + "; access = " + session.getLastAccessedTime());
        session = this.manager.loadSession(id);
        log.info("Loaded session (again):" + session.getId() + "; access = " + session.getLastAccessedTime());
        assertEquals(id, session.getId());
        log.info("Old access time: " + lastAccessed + ";  current access time: " + session.getLastAccessedTime());
        assertTrue(lastAccessed < session.getLastAccessedTime()); // should be 10ms apart

    }


}
