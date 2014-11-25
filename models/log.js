/**
 * Created by sgalytskyy on 20.11.2014.
 */
var azure = require('azure-storage'),
    azureTable = require('azure-table-node'),
    _ = require('underscore');

module.exports = Log;

function Log(storageClient, tableName) {
    this.storageClient = storageClient.getDefaultClient();
    this.tableName = tableName;
}

Log.prototype = {
    find: function (query, callback) {
        var that = this;
        that.storageClient.queryEntities(this.tableName, {
            query:query
        }, function(error, result, response) {
            if (error) {
                callback(error);
            } else {
                callback(null, result, response);
            }
        });
    },

    dropTable: function (callback) {
        var that = this;
        this.storageClient.deleteTable(that.tableName, function (error, response) {
            if (!error) {
                callback(error)
            } else {
                callback(null)
            }
        });

        _.defer(function(){
            that.storageClient.createTableIfNotExists(that.tableName, function tableCreated(error) {
                if (error) {
                    throw error;
                } else {
                    callback(null)
                }
            });
        })
    },

    count : 0,
    batchLimit:255,
    rowList:[],

    setItem: function (item, callback) {
        var that = this;

/*
        var query = new azure.TableQuery()
            .where('RowKey eq ?', item.rKey);
*/

        var query = azureTable.Query.create('RowKey', '==', item.rKey);

        that.partitionKey = item.pKey;

        this.find(query, function (error, data, response) {
            if (error) {
                throw  error;
            } else if (data.length) {
                that.updateblya(item, data, callback);
            } else {
                that.rowList.push(item.rKey);
                that.addblya(item, callback);
            }
        })
    },

    runBatch:function(callback){
        var that = this;

        //reset count
        that.count = 0;

        that.storageClient.executeBatch(that.tableName, that.batch, function (error, result, response) {
            if(error) {
                callback(error);
//                throw error;
            } else {
                console.log('paste batch');
                callback(null);
            }
        });
    },

    addblya: function (item, callback) {
        var that = this,

            itemDescriptor = {
                PartitionKey: that.partitionKey,
                RowKey: item.rKey,
                dT: item.dT,
                oS: item.oS,
                dateArray: item.dateArray,
                dateCount: item.dateCount
            };

        that.storageClient.insertEntity(that.tableName, itemDescriptor, function entityInserted(error, data) {
            that.count++;
            console.log('add item ', that.count, ', ', item.rKey);
            if (error) {
                callback(error);
            }
            callback(null);
        });
    },

    updateblya: function (item, data, callback) {
        var that = this;

        if (item.rKey=='iPhone4,1'){
            var stophere=12;
            console.log('huyaks.me', data[0].dateCount);
        }

        var itemDescriptor = {
            PartitionKey: that.partitionKey,
            RowKey: item.rKey,
            dT: item.dT,
            oS: item.oS,
            dateArray: data[0].dateArray + item.dateArray,
            dateCount: data[0].dateCount+1
        };


        that = this;
/*        that.storageClient.retrieveEntity(that.tableName, that.partitionKey, rKey, function entityQueried(error, entity) {
            if(error) {
                callback(error);
            }
            entity.completed._ = true;

            that.storageClient.updateEntity(that.tableName, entity, function entityUpdated(error) {
                if(error) {
                    callback(error);
                }
                callback(null);
            });
        });*/

        that.storageClient.updateEntity(that.tableName, itemDescriptor,
            {force: true},
            function entityUpdated(error) {
                that.count++;
                console.log('update item ', that.count, ', ', item.rKey);
                if (!!error) {
                    callback(error);
                }
                callback(null);
            });

    }
};
