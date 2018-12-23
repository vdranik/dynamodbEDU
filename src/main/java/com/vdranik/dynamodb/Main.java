package com.vdranik.dynamodb;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudsearchdomain.AmazonCloudSearchDomain;
import com.amazonaws.services.cloudsearchdomain.AmazonCloudSearchDomainClientBuilder;
import com.amazonaws.services.cloudsearchdomain.model.SearchRequest;
import com.amazonaws.services.cloudsearchdomain.model.SearchResult;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.vdranik.dynamodb.dao.CommentDao;
import com.vdranik.dynamodb.dao.ItemDao;
import com.vdranik.dynamodb.domain.Comment;
import com.vdranik.dynamodb.domain.Item;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.EU_CENTRAL_1)
                .build();

        //lowLevelDemo(client);

        //highLevelDemo(client);

        Utils.createTables(client);
        complexQueriesDemo(client);

//        Utils.verifyOrCreateTransactionManager(client);
//        transactionsDemo(client); //need uncomment //transaction style ---  in 'CommentDao' class 'put' method for it

        fulltextDemo();
    }

    private static void lowLevelDemo(AmazonDynamoDB client){
        ItemDao itemDao = new ItemDao(client);
        Item item = new Item();
        item.setId(UUID.randomUUID().toString());
        item.setName("Bitcoin miner");

        itemDao.put(item);
    }

    private static void highLevelDemo(AmazonDynamoDB client) {
        ItemDao itemDao = new ItemDao(client);

        Item item1 = itemDao.put(newItem("Lawn mower", "The very best"));
        Item item2 = itemDao.put(newItem("Apple TV", "Black and white"));
        Item item3 = itemDao.put(newItem("Apple laptop", "With windows XP"));

        print(itemDao.getAll());
        pause();

        itemDao.delete(item2.getId());
        print(itemDao.getAll());
    }

    private static void print(List<?> all) {
        System.out.println(all.stream()
                .map(Object::toString)
                .collect(Collectors.joining("\n")));
    }

    private static Item newItem(String name, String description){
        Item item = new Item();
        item.setName(name);
        item.setDescription(description);

        return item;
    }

    private static void pause(){
        System.out.println("PAUSE");
        try{
            System.in.read();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    private static void complexQueriesDemo(AmazonDynamoDB client){
        CommentDao commentDao = new CommentDao(client);
        removeAll(commentDao);

        Comment c1 = commentDao.put(newComment("1", "Delivered on time", "10", 5));
        Comment c2 = commentDao.put(newComment("1", "Good stuff!", "11", 4));
        Comment c3 = commentDao.put(newComment("1", "Not as described@", "12", 1));
        Comment c4 = commentDao.put(newComment("2", "So-so...", "10", 3));
        Comment c5 = commentDao.put(newComment("3", "Kitten photos here", "10", 5));

        //print(commentDao.getAll());
        //print(commentDao.getAllForItem("1"));
        //print(commentDao.allForItemWithRating("1", 3));
        //print(commentDao.allForUser("10"));

        //optimisticLockingDemo(client);

    }

    private static Comment newComment(String itemId, String msg, String userId, int rating){
        Comment comment = new Comment();
        comment.setItemId(itemId);
        comment.setMessage(msg);
        comment.setUserId(userId);
        comment.setRating(rating);
        return comment;
    }

    private static void removeAll(CommentDao commentDao){
        for(Comment comment: commentDao.getAll()){
            commentDao.delete(comment.getItemId(), comment.getMessageId());
        }
    }

    private static void optimisticLockingDemo(AmazonDynamoDB client){
        ItemDao itemDao = new ItemDao(client);

        Item item = itemDao.put(newItem("Computer", "Good one"));

        Item item1 = itemDao.get(item.getId());
        Item item2 = itemDao.get(item.getId());

        updateName(itemDao, item1);
        updateDescription(itemDao, item2);

        System.out.println(itemDao.get(item.getId()));
    }

    private static void updateName(ItemDao itemDao, Item item){
        item.setName("Apple computer");
        itemDao.update(item);
    }

    private static void updateDescription(ItemDao itemDao, Item item) {
        //optimistic locking simulation
        while (true){
            try {
                item.setDescription("Retina display");
                itemDao.update(item);
                break;
            } catch (ConditionalCheckFailedException ex){
                item = itemDao.get(item.getId());
                System.out.println("Retrying update description request");
            }
        }
    }

    private static void transactionsDemo(AmazonDynamoDB client){
        ItemDao itemDao = new ItemDao(client);
        CommentDao commentDao = new CommentDao(client);

        Item item = itemDao.put(newItem("Apple computer", "Good one"));

        Comment comment = new Comment();
        comment.setItemId(item.getId());
        comment.setMessage("Nice");
        comment.setUserId("10");
        comment.setRating(5);
        comment.setDateTime(LocalDateTime.now());

        commentDao.put(comment);
        System.out.println(itemDao.get(item.getId()));
    }

    private static void fulltextDemo(){
        AmazonCloudSearchDomain cloudSearch =
            AmazonCloudSearchDomainClientBuilder.standard()
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(
                "search-dynamodb-domain-2nzfuakdb53pxirfsr65dyqjve.eu-central-1.cloudsearch.amazonaws.com",
                "eu-central-1"
            )
        ).build();

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.setQuery("mower");

        SearchResult searchResult = cloudSearch.search(searchRequest);

        System.out.println(searchResult);
    }
}
