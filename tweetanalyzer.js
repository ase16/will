'use strict';

const os = require('os');

const events = require('events');
const eventEmitter = new events.EventEmitter();

const config = require('config');
const log = require('winston');
log.level = config.get('log.level');


let batchProcessing = config.get("batchProcessing");
const BACK_OFF_DURATION_WHEN_EMPTY = batchProcessing.backOffDurationWhenEmpty;
const READ_TERMS_INTERVAL = batchProcessing.readTermsInterval;
const SAVE_ANALYSIS_INTERVAL = batchProcessing.saveAnalysisInterval;
let paused = batchProcessing.paused;
const BACK_OFF_DURATION_WHEN_PAUSED = batchProcessing.backOffDurationWhenPaused;

// maps terms to aggregated sentiment
const termToSentiment = {};

let db;
let allTerms = [];

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

function updateTerms(callback) {
    db.getTerms(function(newTerms) {
        allTerms = newTerms;
        if (callback) {
            callback(newTerms);
        }
    });
}
/**
 * Intersection of words and terms, ignoring case.
 * @param words - array of string tokens
 * @returns {Array} - the terms that are present in words
 */
function extractTerms(words) {
    words = words.map(function(s) { return s.toLowerCase().trim(); });
    var terms = [];
    // intersection of all terms and tokens of tweet
    for (var i = 0; i < words.length; i++) {
        for (var k = 0; k < allTerms.length; k++) {
            if (words[i] === allTerms[k]) {
                terms.push(allTerms[k]);
                break;
            }
        }
    }
    return terms;
}

function processTweets() {
    var nodeId = os.hostname();

    if (!paused) {
        db.getTweets(nodeId, function (err, tweets) {
            if (err) {
                log.error("Could not retrieve tweets: ", err);
                return nextTweets();
            }

            var batchSize = tweets.length;
            db.updateLoadOfVM(nodeId, batchSize, function(err) {
                if (!err) {
                    var now = new Date().getTime();
                    var newStat = {
                        created: now,
                        batchSize: batchSize
                    };
                    db.storeStat(newStat, function(err) {
                        if (err) {
                            log.error("Error during writing stats to datastore: err =", err);
                        }
                    });
                }
                else {
                    log.error("Error, something went wrong with updateLoadOfVM: err =", err);
                }
            });

            if (tweets.length === 0) {
                // Got no tweet to analyze - schedule next analysis in a few seconds.
                // We back off a bit in order to not stress the database.
                log.info('No more tweets. Schedule nextTweets in a few seconds');
                setTimeout(nextTweets, BACK_OFF_DURATION_WHEN_EMPTY * 1000);
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
    else {
        setTimeout(nextTweets, BACK_OFF_DURATION_WHEN_PAUSED);
    }
}

function analyzeTweet(tweet) {
    var text = tweet['tweet'];
    log.debug('Got new tweet: %s\t[%s]', tweet['id_str'], text);
    var sentiment = sentimentAnalysis(text);

    var dateTime = new Date(tweet['created_at']);
    var date = new Date(dateTime.getTime());
    date.setHours(0, 0, 0, 0);
    var hour = dateTime.getUTCHours().toString();

    var terms = extractTerms(sentiment.tokens);
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

        setInterval(updateTerms, READ_TERMS_INTERVAL * 1000);

        setInterval(logStats, 10 * 1000);

        setInterval(saveAggregatedSentiments, SAVE_ANALYSIS_INTERVAL * 1000);

        eventEmitter.on('nextTweets', processTweets);

        updateTerms(function(terms) {
            log.debug("Terms: ", terms);
            nextTweets();

            callback();
        });
    }
};

module.exports = tweetanalyzer;