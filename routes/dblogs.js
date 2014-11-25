/**
 * Created by sgalytskyy on 20.11.2014.
 */
var azure = require('azure-storage'),
    async = require('async'),
    _ = require('underscore');

module.exports = DBLog;

function DBLog(log) {
    this.log = log;
}

DBLog.prototype = {
    showLog: function (req, res) {
        var that = this,
            query = new azure.TableQuery().select();

        that.log.find(query, function itemsFound(error, items, response) {
            if (error){
                res.render('db', {title: 'log file', items: []});
            } else {
                var length = _.clone(items).length,
                    count = 450;
                res.render('db', {title: 'log file', length: length,  count: count, items: items.slice(0,count)});
            }
        });
    },

    getPairs: function (string) {
        var array = string.split('='),
            obj = {};

        obj[array[0]] = array[1];

        return obj;

    },

    insertLine: function (line) {
        var that = this;

        if (line) {
            var arr = line.split('&'),
                obj = {};

            for (var i = 0, l = arr.length; i < l; i++) {
                var sArr = arr[i].split('=');
                obj[sArr[0]] = sArr[1];
            }

            var item = {
                pKey: 'dt' + obj.at.replace(/\-/g, '').substr(0, 8),
                rKey: '' + obj.dT,
//                rKey: 'zs' + obj.dHash,
                dT: obj.dT,
                oS: obj.oS,
                dateArray: obj.at,
                dateCount: 1
            };

            that.log.setItem(item, function itemAdded(error) {
                if (error) {
                    throw error;
                }
            })
        }
    },

    parseLog: function (req, res) {
        var that = this;
        //todo: parse log file here

        var fs = require('fs')
            , util = require('util')
            , stream = require('stream')
            , es = require("event-stream");

        var s = fs.createReadStream('./_logs/stat.crop.log.20141117')
            .pipe(es.split())
            .pipe(es.map(function (line) {
                that.insertLine(line);
            })
                .on('error', function () {
                    console.log('Error while reading file.');
                })
                .on('end', function () {
                    res.redirect('/db/');
                    console.log('Read entirefile.')
                })
        );
        return false;
    },

    dropTable:function(req, res){
        var that = this,
            message = 'Are you sure are you want to drop log table?\nThis action cannot be retrieved.';

        that.log.dropTable(function (error) {
            if (error) {
                throw error;
            } else {
                res.redirect('/db/');
            }
        });
    },

// OLD code bellow
    addTask: function (req, res) {
        var that = this,
            item = req.body.item;

        that.task.addItem(item, function itemAdded(error) {
            if (error) {
                throw error;
            }
            res.redirect('/tasks/');
        });
    },

    completeTask: function (req, res) {
        var that = this,
            completedTasks = Object.keys(req.body);

        async.forEach(completedTasks, function taskIterator(completedTask, callback) {
            that.task.updateItem(completedTask, function itemsUpdated(error) {
                if (error) {
                    callback(error);
                } else {
                    callback(null);
                }
            });
        }, function goHome(error) {
            if (error) {
                throw error;
            } else {
                res.redirect('/tasks/');
            }
        });
    }
};