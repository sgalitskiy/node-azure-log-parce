/**
 * Created by sgalytskyy on 20.11.2014.
 */
var azure = require('azure-storage'),
    entityGen = azure.TableUtilities.entityGenerator,
    _ = require('underscore');

module.exports = Log;

function Log(storageClient, tableName, partitionKey) {
    this.storageClient = storageClient;
    this.tableName = tableName;
//    this.partitionKey = partitionKey;

    this.storageClient.createTableIfNotExists(tableName, function tableCreated(error) {
        if (error) {
            throw error;
        }
    });
}

Log.prototype = {
    find: function (query, callback) {
        var that = this;
        that.storageClient.queryEntities(this.tableName, query, null, function entitiesQueried(error, result, response) {
            if (error) {
                callback(error);
            } else {
                callback(null, result.entries, response);
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
    },

    count : 0,
    batchLimit:64,
    rowList:[],
    duplicates:[],
    zi:0,

    setItem: function (item, callback) {
        var that = this;

        if (that.count == 0) {
            that.batch = new azure.TableBatch();
        }

        if (_.find(that.rowList, function(a){return a.rKey == item.rKey})){
            that.duplicates.push(item);
        } else {
            that.rowList.push(item);
            var query = new azure.TableQuery()
                .where('RowKey eq ?', item.rKey);

            that.partitionKey = item.pKey;

            this.find(query, function (error, data, response) {
                if (error) {
                    throw  error;
                } else if (data.length) {
                    //that.updateItem(item, callback, response);
                    that.updateItem(item, data, callback);
                } else {
                    that.addItem(item, callback);
                }
            })
        }
    },

    setDuplicates:function(callback){
        var that = this;
        that.rowList = [];

        _.each(that.duplicates, function(item){
            that.setItem(item,callback);
        });
        _.defer(function(){
            that.duplicates = [];
        })
    },

    runBatch:function(callback){
        var that = this;

        //reset count
        that.count = 0;

        //pasteBatchList
        that.storageClient.executeBatch(that.tableName, that.batch, function (error, result, response) {
            if(error) {
                callback(error);
//                throw error;
            } else {
                console.log('paste batch', that.zi++);
                that.batchLimit = 2;
                that.setDuplicates(callback);
                callback(null);
            }
        });

        _.defer(function(){
            that.batch = new azure.TableBatch();
        });
    },

    addItem: function (item, callback) {
        var that = this,

            itemDescriptor = {
                PartitionKey: entityGen.String(that.partitionKey),
                RowKey: entityGen.String(item.rKey),
                dT: entityGen.String(item.dT),
                oS: entityGen.String(item.oS),
                dateArray: entityGen.String(item.dateArray),
                dateCount: entityGen.Int32(item.dateCount)
            };

        that.batch.insertOrMergeEntity(itemDescriptor, {echoContent:true});
        that.count++;

        if (that.count >=that.batchLimit){
            console.log('add/update', that.count);
            that.runBatch(callback);
        }

/*        that.storageClient.insertOrMergeEntity(that.tableName, itemDescriptor, function entityInserted(error) {
            that.count++;
            console.log('add item ', that.count, ', ', item.rKey);
            if (!!error) {
                callback(error);
            }
            callback(null);
        });
*/
    },

    updateItem: function (item, data, callback) {
        var that = this;

        var itemDescriptor = {
            PartitionKey: entityGen.String(that.partitionKey),
            RowKey: entityGen.String(item.rKey),
            dT: entityGen.String(item.dT),
            oS: entityGen.String(item.oS),
            dateArray: entityGen.String(data[0].dateArray._ + item.dateArray),
            dateCount: entityGen.Int32(data[0].dateCount._+1)
        };

//        itemDescriptor['.metadata'].etag = data['.metadata'].etag;

        that.batch.updateEntity(itemDescriptor,{echoContent:true});
        that.count++;
        console.log('update', that.count);

        if (that.count >=that.batchLimit){
            that.runBatch(callback);
        }

/*        that.storageClient.updateEntity(that.tableName, itemDescriptor,
            function entityUpdated(error) {
                that.count++;
                console.log('update item ', that.count, ', ', item.rKey);
                if (!!error) {
                    callback(error);
                }
                callback(null);
            });
*/
    }
};
