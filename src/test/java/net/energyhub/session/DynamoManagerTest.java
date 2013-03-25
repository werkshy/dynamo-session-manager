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


}
