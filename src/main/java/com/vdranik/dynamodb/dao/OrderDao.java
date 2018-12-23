package com.vdranik.dynamodb.dao;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.vdranik.dynamodb.domain.Order;

import java.util.List;

public class OrderDao {

    private final DynamoDBMapper mapper;

    public OrderDao(AmazonDynamoDB dynamoDb) {
        this.mapper = new DynamoDBMapper(dynamoDb);
    }

    public Order put(Order order) {
        mapper.save(order);

        return order;
    }

    public Order get(String id) {
        return mapper.load(Order.class, id);
    }

    public void update(Order order) {
        mapper.save(order);
    }

    public void delete(String id) {
        Order order = new Order();
        order.setId(id);

        mapper.delete(order);
    }

    public List<Order> getAll() {
        return mapper.scan(Order.class, new DynamoDBScanExpression());
    }
}

