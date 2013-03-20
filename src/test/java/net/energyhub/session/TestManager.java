package net.energyhub.session;

import com.amazonaws.services.dynamodb.AmazonDynamoDB;

/** Test dynamo manager to inject alternator
 */
public class TestManager extends DynamoManager {
    private AmazonDynamoDB dbClient;

    public TestManager(AmazonDynamoDB dbClient) {
        this.dbClient = dbClient;
    }

    @Override
    protected AmazonDynamoDB getDynamo() {
        return dbClient;
    }
}
