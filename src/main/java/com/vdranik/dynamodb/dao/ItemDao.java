package com.vdranik.dynamodb.dao;


import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.vdranik.dynamodb.domain.Item;

import java.util.List;

public class ItemDao {

    private final DynamoDBMapper mapper;

    public ItemDao(AmazonDynamoDB dynamoDB) {
        this.mapper = new DynamoDBMapper(dynamoDB);
    }

    public Item put(Item item) {
        DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
            .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.CLOBBER) //ignore optimistic locking
            .build();

        mapper.save(item, config);
        return item;
    }

    public void update(Item item){
        mapper.save(item);
    }

    public Item get(String id) {
        return mapper.load(Item.class, id);
    }

    public void delete(String id) {
        Item item = new Item();
        item.setId(id);

        mapper.delete(item);
    }

    public List<Item> getAll() {
        return mapper.scan(Item.class, new DynamoDBScanExpression());
    }

    /*
    //LOW LEVEL EXAMPLE
    private final AmazonDynamoDB dynamoDB;

    public ItemDao(AmazonDynamoDB dynamoDB) {
        this.dynamoDB = dynamoDB;
    }

    public void put(Item item){
        Map<String, AttributeValue> itemMap = new HashMap<String, AttributeValue>();
        itemMap.put("id", new AttributeValue().withS(item.getId()));

        if(item.getName() != null){
            itemMap.put("name", new AttributeValue().withS(item.getName()));
        }

        if(item.getDescription() != null){
            itemMap.put("description", new AttributeValue().withS(item.getDescription()));
        }

        itemMap.put("totalRating", new AttributeValue().withN(Integer.toString(item.getTotalRating())));

        itemMap.put("totalComments", new AttributeValue().withN(Integer.toString(item.getTotalComments())));

        PutItemRequest putItemRequest = new PutItemRequest("Items", itemMap);
        dynamoDB.putItem(putItemRequest);
    }
    */

}
