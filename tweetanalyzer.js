'use strict';

var events = require('events');
var eventEmitter = new events.EventEmitter();

const config = require('config');

const Log = require('winston');
Log.level = config.get('log.level');

const db = require('./dbModule.js');

var stats = {};

/**************/
// SENTIMENT -> TODO: make additional module!
// https://github.com/thisandagain/sentiment
var sentiment = require('sentiment');

function sentimentAnalysis(tweet) {
    var result = sentiment(tweet['text']);
    // Log.debug(result);
    return result;
}

// this code is from the other repository.
/*
function performAnalysis(tweetSet) {
    //set a results variable
    var results = 0;
    // iterate through the tweets, pulling the text, retweet count, and favorite count
    for(var i = 0; i < tweetSet.length; i++) {
        var tweet = tweetSet[i]['text'];
        var retweets = tweetSet[i]['retweet_count'];
        var favorites = tweetSet[i]['favorite_count'];
        // remove the hastag from the tweet text
        tweet = tweet.replace('#', '');
        // perform sentiment on the text
        var score = sentiment(tweet)['score'];
        // calculate score
        results += score;
        if(score > 0){
            if(retweets > 0) {
                results += (Math.log(retweets)/Math.log(2));
            }
            if(favorites > 0) {
                results += (Math.log(favorites)/Math.log(2));
            }
        }
        else if(score < 0){
            if(retweets > 0) {
                results -= (Math.log(retweets)/Math.log(2));
            }
            if(favorites > 0) {
                results -= (Math.log(favorites)/Math.log(2));
            }
        }
        else {
            results += 0;
        }
    }
    // return score
    results = results / tweetSet.length;
    return results;
}
*/

/**************/

function extractWords(text) {
    var re = /\w+/gi;
    var words = text.match(re)
    /*
    words.forEach(function(w) {
        Log.info('\t%s', w);
    });
    */
    return words;
}



function analyzeTweet() {
    db.findSingleNewTweet(function(err, res) {
        if (err) {
            Log.warn(err);
            nextTweet();
        } else {
            if(!res.value) {
                // got no tweet to analyze - schedule next analysis in a few seconds.
                // we back off a bit in order to not stress the database.
                Log.info('No more tweets. Schedule nextTweet in a few seconds');
                setTimeout(nextTweet, 5*1000);
                // TODO: maybe exit and shutdown if no work or too little work for too long
            } else {
                Log.debug('Got new tweet: %s\t[%s]', res.value['id_str'], res.value['text']);
                var tweet = res.value;
                var words = extractWords(tweet['text']); // TODO: we can get the words from the sentiment analysis...
                var sentimentResult = sentimentAnalysis(tweet);
                
                db.updateSingleTweetWithAnalysis(res.value['_id'], words, sentimentResult, function(err, res) {
                    if (err) {
                        Log.error('Could not update tweet', err);
                    } else {
                        Log.debug('Tweet analyzed.');
                    }
                    nextTweet();
                });
            }
        }
    });
}

// Indicate that we are ready to analyze the next tweet
function nextTweet() {
    stats['numberOfTweetsAnalyzed'] += 1;
    eventEmitter.emit('nextTweet');
}

function logStats() {
    var now = new Date().getTime();
    var newTweets = stats['numberOfTweetsAnalyzed']-stats['numberOfTweetsAnalyzedLog'];
    var timeSpan = now-stats['timestamp'];
    var tweetspersec = (newTweets/timeSpan*1000).toFixed(1);
    if (isNaN(tweetspersec)) { tweetspersec=0 }
    stats['timestamp'] = now;
    stats['numberOfTweetsAnalyzedLog'] = stats['numberOfTweetsAnalyzed'];
    Log.info('Processed %d tweets in %d s, %s tweets/s, total tweets processed is %d',
        newTweets, (timeSpan/1000).toFixed(1), tweetspersec, stats['numberOfTweetsAnalyzed']);
}

function setupStats() {
    // numberOfTweetsAnalyzed: this is the total and incremented with each tweet that is analyzed
    // numberOfTweetsAnalyzedLog: this is set when the stats are printed (i.e. total of previous interval).
    stats = {
        timestamp: new Date().getTime(),
        numberOfTweetsAnalyzed: 0,
        numberOfTweetsAnalyzedLog: 0
    };
    setInterval(logStats, 10*1000);
}

db.connect(function(err, res) {
    Log.debug("Connected to database.");
    setupStats();

    eventEmitter.on('nextTweet', analyzeTweet);
    nextTweet();
});