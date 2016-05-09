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
    getTweet: function(callback) {
        if (!isConnected()) {
            callback("Not connected", null);
            return;
        }

        var query = datastore.createQuery('Tweet').autoPaginate(false).limit(2000);
        datastore.runQuery(query, callback);
    }
};

module.exports = db;