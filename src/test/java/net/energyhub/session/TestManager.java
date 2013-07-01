package net.energyhub.session;

import static org.mockito.Mockito.*;

import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import org.apache.catalina.Container;
import org.apache.catalina.Pipeline;
import org.apache.catalina.Valve;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import java.util.ArrayList;


/** Test dynamo manager to inject alternator
 */
public class TestManager extends DynamoManager {
    private static Log log = LogFactory.getLog(TestManager.class);
    private AmazonDynamoDB dbClient;

    public TestManager(AmazonDynamoDB dbClient) {
        this.dbClient = dbClient;
    }

    @Override
    protected AmazonDynamoDB getDynamo() {
        return dbClient;
    }

    /*
    Stub and mock enough to make the container work in tests
     */
    @Override
    public Container getContainer() {
        Container container = mock(Container.class);
        Pipeline pipeline = mock(Pipeline.class);
        when(pipeline.getValves()).thenReturn(new Valve[]{});
        when(container.getPipeline()).thenReturn(pipeline);

        when(container.getLogger()).thenReturn(log);
        return container;
    }

}
