/**
 * Created by sgalytskyy on 19.11.2014.
 */
var azure = require('azure-storage'),
    async = require('async');

module.exports = TaskList;

function TaskList(task) {
    this.task = task;
}

TaskList.prototype = {
    showTasks: function(req, res) {
        self = this;
        var query = new azure.TableQuery()
            .where('completed eq ?', false);
        self.task.find(query, function itemsFound(error, items) {
            res.render('task',{title: 'My ToDo List ', tasks: items});
        });
    },

    addTask: function(req,res) {
        var self = this;
        var item = req.body.item;

        self.task.addItem(item, function itemAdded(error) {
            if(error) {
                throw error;
            }
            res.redirect('/tasks/');
        });
    },

    completeTask: function(req,res) {
        var self = this;
        var completedTasks = Object.keys(req.body);
        async.forEach(completedTasks, function taskIterator(completedTask, callback) {
            self.task.updateItem(completedTask, function itemsUpdated(error) {
                if(error){
                    callback(error);
                } else {
                    callback(null);
                }
            });
        }, function goHome(error){
            if(error) {
                throw error;
            } else {
                res.redirect('/tasks/');
            }
        });
    }
};