const { 
    createConnection, 
    getCollections, 
    syncCollection 
} = require("./helpers");

const source = {
    url: "mongodb://localhost:27018",
    name: "adp-dev" 
}

const dest = {
    url: "mongodb://localhost:27017",
    name: "adp-dev"
}

const run = async () => {
    try {
        const sourceDb = await createConnection(source.url, source.name);
        const destDb = await createConnection(dest.url, dest.name);
    
        const sourceCollections = await getCollections(sourceDb);
    
        const syncColectionsPromises = sourceCollections.map(collection => syncCollection(sourceDb, destDb, collection));
        await Promise.all(syncColectionsPromises);
        console.log("Data successfully migrated!");
    } catch (e) {
        console.log(`Error: ${e}`);
    }
}

run();



