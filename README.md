
# Loading data to CosmosDB

Javascript code for loading a graph into Cosmos DB and measuring the time it takes. It uses node.js and the [gremlin-javascript module](https://github.com/jbmusso/gremlin-javascript). The design is inspired from [Azure-samples](https://github.com/Azure-Samples/azure-cosmos-db-graph-nodejs-getting-started)

## Configuration

First, you must put the information for connecting to CosmosDB (graph endpoint, primary key, name of the DB and of the collection) in `config.js`.

Second, a `json` file containing a graph must be present in the folder. You may find the original one in the following repository:
[bricaud/tree-of-life-dataset](https://github.com/bricaud/tree-of-life-dataset). It contains a large graph representing the tree of life, see the file `data/treeoflife.json`.

Optionally, you may change the data filename inside the code (see next step).

## Testing the loading speed by loading nodes and edges, one by one.

Once configured, you may run the code using node.js:

```
node app.js
```

## Loading nodes in parallel, then edges in parallel

The code for loading in parallel uses the javascript module `async`.

```
node loadasync.js
```
The loading speed is much higher as it leverage the DB hability to handle multiple requests in parallel.


This code is open-source, Apache 2.0 license.



