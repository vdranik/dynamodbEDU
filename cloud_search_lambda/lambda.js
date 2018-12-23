'use strict';
var aws = require('aws-sdk');

console.log('Loading function');

exports.handler = (event, context, callback) => {
    var cloudSearchClient = new aws.CloudSearchDomain({
        endpoint: 'doc-shop-domain-ft32cd2vtsf4r42cqim4oopvwe.us-east-1.cloudsearch.amazonaws.com'
    });

    var cloudSearchDocuments = event.Records.map(function(record) {
        console.log('Processing record: ' + JSON.stringify(record));
        var id = record.dynamodb.Keys.id.S
        if (record.eventName == 'REMOVE') {
            return {
                id: id,
                type: 'delete'
            };
        }
        
        var image = record.dynamodb.NewImage;
        
        return {
            id: id,
            type: 'add',
            fields: {
                description: image.description.S,
                totalcomments: image.totalComments.N,
                version: image.version.N,
                totalrating: image.totalRating.N,
                id: image.id.S,
                name: image.name.S
            }
        };
    });
    
    var uploadDocumentsParams = {
        contentType: 'application/json',
        documents : JSON.stringify(cloudSearchDocuments)
    };
    
    cloudSearchClient.uploadDocuments(uploadDocumentsParams, function(err, data) {
        if(err) {
            console.log('Failed to process documents', err, err.stack);
            context.fail(err);
        } else {
            context.succeed("Synchronized " + event.Records.length + " records.");  
        }
    });
};
