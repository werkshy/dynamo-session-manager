package net.energyhub.session;

import static org.junit.Assert.*;

import org.apache.catalina.Session;
import org.apache.catalina.session.StandardSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodb.model.*;
import com.michelboudreau.alternator.AlternatorDB;
import com.michelboudreau.alternator.AlternatorDBClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DynamoManagerTest {
    private static Logger log = Logger.getLogger(DynamoManagerTest.class.getName());

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
        log.setLevel(Level.FINE);
        Logger.getLogger(DynamoManager.class.getName()).setLevel(Level.FINE);
        Logger.getLogger("org.apache.http.wire").setLevel(Level.INFO);
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

        Thread.sleep(1000l);

        session = this.manager.loadSession(id);
        log.info("Loaded session:" + session.getId() + "; access = " + session.getLastAccessedTime());
        // verify that loaded session still has the last accessed time we expect
        assertEquals(id, session.getId());
        long lastAccessed = session.getLastAccessedTime();

        // Now save again and verify that the last accessed time has changed
        this.manager.save(session);
        log.info("Saved session (again):" + session.getId());
        session = this.manager.loadSession(id);
        log.info("Loaded session (again):" + session.getId() + "; access = " + session.getLastAccessedTime());
        assertEquals(id, session.getId());
        log.info("Old access time: " + lastAccessed + ";  current access time: " + session.getLastAccessedTime());
        assertTrue(lastAccessed < session.getLastAccessedTime()); // should be 10ms apart

    }

    @Test
    public void testHaveAttributesChanged() throws Exception {
        Map<String, Object> originalAttributes = new HashMap();
        originalAttributes.put("FOO", "BAR");
        originalAttributes.put("FOO2", "BAR2");

        // start off with a session with the same attributes
        StandardSession session = setUpSession(originalAttributes);
        // modify the session attributes
        session.getSession().setAttribute("FOO", "BAZ");
        // attribute has changed
        assertTrue(this.manager.haveAttributesChanged(originalAttributes, session));

        // Get another instance of session and add new attribute
        session = setUpSession(originalAttributes);
        session.getSession().setAttribute("FOO3", "QUX");
        // attributes have changed
        assertTrue(this.manager.haveAttributesChanged(originalAttributes, session));

        // Get another instance of session and remove attribute
        session = setUpSession(originalAttributes);
        session.getSession().removeAttribute("FOO2");
        // attributes have changed
        assertTrue(this.manager.haveAttributesChanged(originalAttributes, session));

        // Get a new instance of this session and do not modify it
        session = setUpSession(originalAttributes);
        // attribute has not changed
        assertFalse(this.manager.haveAttributesChanged(originalAttributes, session));

        // Get a new instance of this session and set the same attrib
        session = setUpSession(originalAttributes);
        session.getSession().setAttribute("FOO", "BAR");
        // attribute has not changed
        assertFalse(this.manager.haveAttributesChanged(originalAttributes, session));
    }


    protected StandardSession setUpSession(Map<String, Object> attributes) {
        StandardSession originalSession = (StandardSession) this.manager.createSession();
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            originalSession.getSession().setAttribute(entry.getKey(), entry.getValue());
        }
        return originalSession;
    }
}
