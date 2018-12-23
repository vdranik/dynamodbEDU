package com.vdranik.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.vdranik.dynamodb.dao.ItemDao;
import com.vdranik.dynamodb.domain.Item;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class TestItemDao {
    static AmazonDynamoDB dynamoDB;
    static ItemDao itemDao;

    @Before
    public void before(){
        dynamoDB = DynamoDBEmbedded.create().amazonDynamoDB();
        Utils.createTables(dynamoDB);
        itemDao = new ItemDao(dynamoDB);
    }

    @Test
    public void testDynamoDB(){
        Item item = itemDao.put(new Item());
        assertNotNull(itemDao.get(item.getId()));
    }
}
