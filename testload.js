"use strict";
var config = require("./config");
var Gremlin = require('gremlin');
var loadjson = require("./loadasync")

// Filename to load
var filename = 'treeoflife.json';
var filename = 'treeoflife_subset.json';



// Connect to the server
const client = Gremlin.createClient(
    443, 
    config.endpoint, 
    { 
        "session": false, 
        "ssl": true, 
        "user": `/dbs/${config.database}/colls/${config.collection}`,
        "password": config.primaryKey
    });

console.log('Loading data...')
loadjson.loadjson(filename,client);

