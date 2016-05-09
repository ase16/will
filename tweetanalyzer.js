'use strict';

var events = require('events');
var eventEmitter = new events.EventEmitter();

const config = require('config');
const log = require('winston');
log.level = config.get('log.level');

let db;

// #tweets and ts when last updated.
var stats = {
    timestamp: new Date().getTime(),
    tweetsCount: 0
};

/**************/
// SENTIMENT
// https://github.com/thisandagain/sentiment
var sentiment = require('sentiment');

function sentimentAnalysis(text) {
    var result = sentiment(text);
    // log.debug(result);
    return result;
}

function extractWords(text) {
    var re = /\w+/gi;
    var words = text.match(re)
    /*
    words.forEach(function(w) {
        log.info('\t%s', w);
    });
    */
    return words;
}



function analyzeTweet() {
    db.getTweet(function (err, tweets) {
        if (err) {
           log.warn(err);
           return nextTweet();
        }

        if (tweets.length === 0) {
            // got no tweet to analyze - schedule next analysis in a few seconds.
            // we back off a bit in order to not stress the database.
            log.info('No more tweets. Schedule nextTweet in a few seconds');
            setTimeout(nextTweet, 10*1000);
            return;
        }

        tweets.forEach(function(t) {
            var tweet = t.data;
            log.debug('Got new tweet: %s\t[%s]', t['id_str'], t['tweet']);
            var text = tweet['tweet'];
            var words = extractWords(text);
            var sentimentResult = sentimentAnalysis(text);
            stats.tweetsCount += 1;

            /*db.updateSingleTweetWithAnalysis(res.value['_id'], words, sentimentResult, function(err, res) {
                if (err) {
                    log.error('Could not update tweet', err);
                } else {
                    log.debug('Tweet analyzed.');
                }
                nextTweet();
            });
            */

        });
        nextTweet();
    });
}

// Indicate that we are ready to analyze the next tweet
function nextTweet() {
    eventEmitter.emit('nextTweet');
}

function logStats() {
    var now = new Date().getTime();
    var timeSpan = now-stats.timestamp;
    var newTweets = stats.tweetsCount;
    var tweetsPerSec = (newTweets/timeSpan*1000).toFixed(1);
    if (isNaN(tweetsPerSec)) { tweetsPerSec=0.0 }
    stats.timestamp = now;
    stats.tweetsCount = 0;

    log.info('Processing ' + tweetsPerSec + ' tweets/second')
}

const tweetanalyzer = {
    init: function(dbModule, callback) {
        db = dbModule;

        setInterval(logStats, 10*1000);

        eventEmitter.on('nextTweet', analyzeTweet);
        nextTweet();
    }
};

module.exports = tweetanalyzer;