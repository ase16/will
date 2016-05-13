'use strict';

const os = require('os');

const events = require('events');
const eventEmitter = new events.EventEmitter();

const config = require('config');
const log = require('winston');
log.level = config.get('log.level');

// maps terms to aggregated sentiment
const termToSentiment = {};

let db;

// #tweets and ts when last updated.
const stats = {
    timestamp: Date.now(),
    tweetsCount: 0
};

/**************/
// SENTIMENT
// https://github.com/thisandagain/sentiment
const sentiment = require('sentiment');

function sentimentAnalysis(text) {
    var result = sentiment(text);
    // log.debug(result);
    return result;
}

function getOrCreateAggregatedSentiment(term, date) {
    if (!termToSentiment.hasOwnProperty(term)) {
        termToSentiment[term] = {};
    }

    if (!termToSentiment[term].hasOwnProperty(date)) {
        termToSentiment[term][date] = createEmptyAggregatedSentiment(term, date);
    }

    return termToSentiment[term][date].data;
};

function createEmptyAggregatedSentiment(term, date) {
    var key = db.createKeyForTerm(term);
    var sentiment = {
        key: key,
        data: {
            created: Date.now(),
            date: date,
            term: term,
            totalTweets: 0,
            hourBuckets : {
                "0": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "1": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "2": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "3": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "4": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "5": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "6": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "7": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "8": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "9": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "10": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "11": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "12": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "13": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "14": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "15": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "16": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "17": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "18": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "19": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "20": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "21": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "22": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                },
                "23": {
                    numTweets: 0,
                    aggrSentiment: 0.0,
                    positive: null,
                    negative: null
                }
            }
        }
    };

    return sentiment;
}

function processTweets() {
    var nodeId = os.hostname();
    db.getTweets(nodeId, function (err, tweets) {
        if (err) {
           log.error("Could not retrieve tweets: ", err);
           return nextTweets();
        }

        if (tweets.length === 0) {
            // got no tweet to analyze - schedule next analysis in a few seconds.
            // we back off a bit in order to not stress the database.
            log.info('No more tweets. Schedule nextTweets in a few seconds');
            setTimeout(nextTweets, 10*1000);
            return;
        }

        tweets.forEach(function(t) {
            analyzeTweet(t.data);
            stats.tweetsCount += 1;
        });

        db.deleteTweets(tweets, function(err) {
            if (err) {
                log.error("Could not delete tweets: ", err);
            }
            nextTweets();
        });
    });
}

function analyzeTweet(tweet) {
    var text = tweet['tweet'];
    log.debug('Got new tweet: %s\t[%s]', tweet['id_str'], text);
    var sentiment = sentimentAnalysis(text);

    // TODO: should be the intersection of the tokens of the tweet and all terms in DB!
    var allTerms = []; // TODO: get all terms from DB
    var terms = [];
    /*
    // intersection of all terms and tokens of tweet
    for (var i = 0; i < sentiment.tokens.length; i++) {
        for (var k = 0; k < allTerms.length; k++) {
            if (sentiment.tokens[i] == allTerms[k]) {
                terms.push(i);
                break;
            }
        }
    }
    */

    terms = ['testterm']; // TODO: remove once code above works correctly.

    var dateTime = new Date(tweet['created_at']);
    var date = new Date(dateTime.getTime());
    date.setHours(0, 0, 0, 0);
    var hour = dateTime.getUTCHours().toString();

    terms.forEach(function(term) {
        var sentimentStats = getOrCreateAggregatedSentiment(term, date);
        sentimentStats.totalTweets += 1;
        // update the sentimentStats value
        sentimentStats.hourBuckets[hour].numTweets += 1;
        sentimentStats.hourBuckets[hour].aggrSentiment += sentiment.comparative;

        // create histogram if not exists yet
        if (sentiment.positive.length > 0) {

            if (sentimentStats.hourBuckets[hour].positive === null) {
                sentimentStats.hourBuckets[hour].positive = {};
            }
        }
        // update histogram of positive words
        sentiment.positive.forEach(function(posWord) {
            if (!sentimentStats.hourBuckets[hour].positive.hasOwnProperty(posWord)) {
                sentimentStats.hourBuckets[hour].positive[posWord] = 0;
            }
            sentimentStats.hourBuckets[hour].positive[posWord] += 1;
        });

        // create histogram if not exists yet
        if (sentiment.negative.length > 0) {
            if (sentimentStats.hourBuckets[hour].negative === null) {
                sentimentStats.hourBuckets[hour].negative = {};
            }
        }
        // update histogram of negative words
        sentiment.negative.forEach(function(posWord) {
            if (!sentimentStats.hourBuckets[hour].negative.hasOwnProperty(posWord)) {
                sentimentStats.hourBuckets[hour].negative[posWord] = 0;
            }
            sentimentStats.hourBuckets[hour].negative[posWord] += 1;
        });
    });
}

// Indicate that we are ready to analyze the next tweet
function nextTweets() {
    eventEmitter.emit('nextTweets');
}

function saveAggregatedSentiments() {
    db.upsertAggregatedSentiment(termToSentiment, function() {
        log.info("Saved sentiment statistics.");
    });
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

        setInterval(saveAggregatedSentiments, 10*1000);

        eventEmitter.on('nextTweets', processTweets);
        nextTweets();
    }
};

module.exports = tweetanalyzer;