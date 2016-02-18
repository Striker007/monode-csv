var async = require('async');
var MongoClient = require('mongodb').MongoClient;
console.dir('_______________________________________________/');

MongoClient.connect('mongodb://__IP__/reports', function (err, db) {
    if (err) throw err;

    var start = 1446328800, end = 1451599199, next = 0;

    var addOffsetOneDay = function (timestamp) {
        return timestamp + 86400;
    };

    var getStatistic = function (start, end, callback) {
        db.collection('cardEnds').aggregate([
            {$match: {'t': {$gt: start, $lte: end}}},
            {$limit: 100},
            {$group: {_id: '$u', cards: {$sum: 1}}},
            {$group: {_id: null, cards: {$avg: '$cards'}}}
        ]).toArray(function (err, result) {

            if (err) throw err;

            callback(null, result);

        });
    };

    var writeToCSV = function (data) {
        var fs = require('fs');
        var csv = require('csv-write-stream');
        //var writer = csv({sendHeaders: false});
        var writer = csv({ headers: ["_id", "cards"]});
        writer.pipe(fs.createWriteStream('out.csv'));
        data.forEach(function (item) {
            writer.write(item[0]);
            console.log(item[0]);
        });
        writer.end();
    };

    var tasks = [];
    while (start < end) {
        next = addOffsetOneDay(start);
        tasks.push(function (s, e) {
            return function (callback) {
                getStatistic(
                    s,
                    e,
                    callback
                );
            }
        }(start, end));
        start = addOffsetOneDay(start);
    }

    // RUN
    async.parallel(tasks, function (err, results) {
        if (err) throw err;
        db.close();
        console.dir('_______write_to_csv_____________________________/');
        writeToCSV(results);
        console.dir('------DONE-----');
    });


});



