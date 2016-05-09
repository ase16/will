'use strict';
const config = require('config');
const log = require('winston');
log.level = config.get('log.level');

const db = require('./datastore.js');
const tweetanalyzer = require('./tweetanalyzer.js');

log.info('TweetAnalyzer initializing...');
db.connect(config.get('gcloud'), function() {
    tweetanalyzer.init(db, function() {
        log.info('TweetAnalyzer is ready.');
    });
});