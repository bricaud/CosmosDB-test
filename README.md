
# Loading data to CosmosDB

Code for loading a graph into Cosmos DB and measuring the time it takes.

First, you must put the information for connecting to CosmosDB (graph endpoint, primary key, name of the DB and of the collection) in `config.js`.

Second, a `json` file containing a graph must be present in the folder. You may find the original one in the following repository:
[bricaud/tree-of-life-dataset](https://github.com/bricaud/tree-of-life-dataset). It contains a large graph representing the tree of life, see the file `data/treeoflife.json`.

Third, run the code using node.js:
```
node app.js
```

This code is open-source, Apache 2.0 license.

