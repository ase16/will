'use strict';
const gcloud = require('gcloud');
const config = require('config');
const log = require('winston');
log.level = config.get('log.level');

// Authenticating on a per-API-basis.
let datastore;

// this is a pseudo-check to see if we have a connection
// if the object is not undefined, we assume it has been initialized/set
const isConnected = () => typeof datastore !== "undefined";


const db = {
    connect: function(conf, callback) {
        // do we already have a connection?
        if (isConnected()) return;
        datastore = gcloud.datastore(conf);
        callback();
    },

    getTerms: function(callback) {
        if (!isConnected()) {
            return callback("Not connected", null);
        }

        var query = datastore
            .createQuery('Term')
            .select('__key__')
            .autoPaginate(true);

        var terms = [];
        datastore.runQuery(query)
            .on('error', log.error)
            .on('data', function (entity) {
                terms.push(entity.key.name.toLowerCase().trim());
                // this.end(); // allows early termination
            })
            .on('end', function() {
                    callback(terms);
                });
    },

    getTweets: function(nodeId, callback) {
        if (!isConnected()) {
            return callback("Not connected", null);
        }
        var query = datastore
            .createQuery('Tweet')
            .autoPaginate(false)
            //.filter('vm', '=', nodeId)
            .limit(500);
        datastore.runQuery(query, callback);
    },

    updateTweets: function(tweets, callback) {
        datastore.update(tweets, callback);
    },

    deleteTweets: function(tweets, callback) {
        if (!isConnected()) {
            return callback("Not connected", null);
        }
        
        var keys = [];
        tweets.forEach(function(t) {
            keys.push(t.key);
        });
        datastore.delete(keys, callback);
    },

    createKeyForTerm: function (term) {
        return datastore.key(['sentiment_' + term]);
    },

    upsertAggregatedSentiment: function(sentiments, callback) {
        if (!isConnected()) {
            return callback("Not connected.");
        }

        for (var term in sentiments) {
            if (sentiments.hasOwnProperty(term)) {
                for (var date in sentiments[term]) {
                    if (sentiments[term].hasOwnProperty(date)) {
                        datastore.upsert(sentiments[term][date], function(err) {
                            if (err) {
                                log.error("Could not save sentiment");
                            }
                        });
                    }
                }
            }
        }
        callback(null);
    }
};

module.exports = db;