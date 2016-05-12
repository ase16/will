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

    // Add a new tweet.
    // @param tweet JSON as received from the Twitter API
    // @param callback fn(err, res)
    getTweets: function(nodeId, callback) {
        if (!isConnected()) {
            callback("Not connected", null);
            return;
        }
        var query = datastore
            .createQuery('Tweet')
            .autoPaginate(false)
            .filter('vm', '=', nodeId)
            .limit(500);
        datastore.runQuery(query, callback);
    },

    updateTweets: function(tweets, callback) {
        datastore.update(tweets, callback);
    }
};

module.exports = db;