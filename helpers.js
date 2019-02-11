
const { MongoClient } = require('mongodb');

const createConnection = (url, dbName) => {
    return new Promise((resolve, reject) => {
        MongoClient.connect(url, { 
            useNewUrlParser: true, 
            socketTimeoutMS: 0,
            keepAlive: true,
            reconnectTries: 30,
            poolSize: 20, 
        }, (err, client) => {
            if (err) reject(err);
            console.log(`Connected successfully to server: ${url}`);
            resolve(client.db(dbName));
        });
    })
}

const insertDocuments = (db, collectionName, data) => {
    return new Promise((resolve, reject) => {
        const collection = db.collection(collectionName);
        collection.insertMany(data, (err, result) => {
            if (err) reject(err);
            resolve(result);
        })
    })
}

const findDocuments = (db, collectionName, filters) => {
    return new Promise((resolve, reject) => {
        const collection = db.collection(collectionName);
        collection.find(filters).sort({createdAt: -1}).toArray((err, docs) => {
          if (err) reject(err);
          resolve(docs);
        });
    })
}

const findLatestDocument = (db, collectionName) => {
    return new Promise((resolve, reject) => {
        const collection = db.collection(collectionName);
        collection.find({}).sort({createdAt: -1}).limit(1).toArray((err, docs) => {
          if (err) reject(err);
          resolve(docs);
        });
    })
}

const getCollections = (db) => {
    return new Promise((resolve, reject) => {
        db.listCollections().toArray((err, collInfos) => {
           if (err) reject(err);
           resolve(collInfos.map(item => item.name));
        });
    })
}


const syncCollection = async (sourceDb, destDb, collection) => {
    const destLatestDocument = await findLatestDocument(destDb, collection);
    const filters = (destLatestDocument.length) ? { createdAt: { $gt: destLatestDocument[0].createdAt } } : {};
    const newData = await findDocuments(sourceDb, collection, filters);
    if (newData.length) {
        const result = await insertDocuments(destDb, collection, newData);
        console.log(`Collection: ${collection}, added ${newData.length} documents`);
    } else {
        console.log(`Collection: ${collection}, new documents not found`);
    }
}

module.exports = {
    createConnection,
    getCollections,
    syncCollection
}