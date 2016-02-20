var async = require('async');
var MongoClient = require('mongodb').MongoClient;
console.dir('_______________________________________________/');

MongoClient.connect('mongodb://__SomeDB_IP__:27017/reports', function (err, db) {
    if (err) throw err;

    var startPeriod = 1446328800,
        endPeriod = 1451599199,
        startDay = startPeriod,
        endDay = 0,
        addOffsetOneDay,
        getStatistic,
        query = [],
        writeToCSV,
        tasks;

    addOffsetOneDay = function (timestamp) {
        return timestamp + 86400;
    };

    getStatistic = function (start, end, query, callback) {
        db.collection('cardEnds').aggregate(query).toArray(function (err, result) {
            if (err) throw err;
            callback(null, result);
        });
    };

    writeToCSV = function (data) {
        var fs = require('fs');
        var csv = require('csv-write-stream');
        var writer = csv({headers: ["_id", "cards"]});
        writer.pipe(fs.createWriteStream('out.csv'));
        data.forEach(function (item) {
            writer.write(item[0]);
            console.log(item[0]);
        });
        writer.end();
    };


    // RUN
    tasks = [];
    query[5] = [
        {
            $match: {
                't': {$gt: start, $lte: end},
                'p.state': {$nin: ['skipped', 'exit', 'forceExit']}
            }
        },
        {$group: {_id: '$u', cards: {$sum: 1}}},
        {$group: {_id: null, cards: {$avg: '$cards'}}}
    ];
    query[6] = [];

    while (endDay < endPeriod) {
        endDay = addOffsetOneDay(startDay);
        tasks.push(function (s, e, q) {
            return function (callback) {
                getStatistic(
                    s,
                    e,
                    q,
                    callback
                );
            }
        }(startDay, endDay, query[5]));
        startDay = addOffsetOneDay(startDay);
        console.log('.');
    }

    async.parallel(tasks, function (err, results) {
        if (err) throw err;
        db.close();
        console.dir('_______write_to_csv_____________________________/');
        writeToCSV(results);
        console.dir('------DONE-----');
    });


});
