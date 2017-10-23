"use strict";

var Gremlin = require('gremlin');
var config = require("./config");
var fs = require('fs');
var async = require('async');

//import eachLimit from 'async/eachLimit';

var node_data;
var edge_data;



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



// Display the number of nodes (async)
//query_nb_nodes()

// Main function: read data from the file and write the the database
async.waterfall([
    initProcess,
    eraseDB,
    readDataFile,
    writeTheNodes,
    retry_missed_nodes,
    endNodeProcess,
    get_edge_data,
    writeTheEdges,
    retry_missed_edges,
    endProcess
  ], function (err, result) {
    if (err) return console.error(err);
    console.log('Process done.');
  }
);


var filename = 'treeoflife.json';
//var filename = 'treeoflife_subset.json';

function readDataFile(callback) {
  fs.readFile(filename, 'utf8', function (err, data) {
    if (err) throw err;
    var obj = JSON.parse(data);
    node_data = obj['nodes'];
    edge_data = obj['links'];
    console.log('Examples of data structure:')
    console.log('For nodes:')
    console.log(node_data[0])
    console.log('For edges:')
    console.log(edge_data[0])
 
    // Launch the iterative process
    // The nodes and edges are written sequentially

    callback(null,node_data);

    // A command closing the connection is missing, like:
    //client.closeConnection();

  });
}


var start_time = new Date().getTime();
function initProcess(callback){
  // Measure the time to load the data
  start_time = new Date().getTime();
  callback();
}

function endProcess(callback){
  console.log('Nodes and edges written.')
  setInterval(regular_info,30000)
  // Check lost nodes
  //console.log('Number of lost nodes: ' + Object.keys(lost_nodes).length)
  callback();
}


var time_since_start = 0

function regular_info(){
  var nb_nodes_to_write = Object.keys(node_data).length;
  console.log('Nb of nodes in file: ' + nb_nodes_to_write)
  var nb_nodes_written = Object.keys(node_index_table).length;
  console.log('Nb of nodes written: '+ nb_nodes_written);
  var nb_nodes_missed = Object.keys(lost_nodes).length;
  console.log('Nb of nodes lost: '+ nb_nodes_missed);
  var nb_nodes_timed_out = Object.keys(timeout_node_list).length;
  console.log('Nb of node requests that timed out: ' + nb_nodes_timed_out )
  console.log('written + lost + timed_out: ' + (nb_nodes_written + nb_nodes_missed + nb_nodes_timed_out) )
  console.log('------------------------------------')
  var nb_edges_to_write = Object.keys(edge_data).length;
  console.log('Nb of edges in file: ' + nb_edges_to_write)
  console.log('Nb of edges written: '+ nb_edges_being_written);
  var nb_edges_missed = Object.keys(lost_edges).length;
  console.log('Nb of edges lost: '+ nb_edges_missed);
  var nb_edges_timed_out = Object.keys(timeout_edge_list).length;
  console.log('Nb of edge requests that timed out: ' + nb_edges_timed_out )  
  console.log('written + lost + timed_out: ' + (nb_edges_being_written + nb_edges_missed + nb_edges_timed_out) )

  // Query the number of nodes and edges (asynchroneous!)  
  var nodes_in_database = query_nb_nodes_edges();
  console.log('Time since started: ' + time_since_start + ' seconds, or ' + (time_since_start/60) + 'min')
  time_since_start += 30
}


////////////////////////////////////////////////////////////////////////////////
// Functions to save the node
////////////////////////////////////////////////////////////////////////////////
var node_index_table = {};
var nb_nodes_being_written = 0;
var lost_nodes = {};
var timeout_node_list = {};



function writeTheNodes(node_data, callback){
  console.log('')
  console.log('Start writing nodes to the database.')
  async.eachOfLimit(node_data,40, wrapped_timeout_writeNode, function(err, result){
    // if any of the saves produced an error, err would equal that error
    if (err) {
      console.error(err);
      callback();
      return;
    }
    var time_step = new Date().getTime();
    var diff_time2 = (time_step - start_time)
    console.log("Time spent so far: " + diff_time2 + "ms or " + (diff_time2/1000/60) + " min.");
    callback();
  });
}


function writeNode(node, node_key, callback){
  console.log(node_key)
  var gremlin_query = "g.addV('species').property('node_id', node_id).property('name', name)" +
      ".property('phylesis', phylesis).property('extinct', extinct).property('confidence', confidence)" +
      ".property('childcount', childcount).id()";
  var gremlin_bindings = { node_id: node['ID'], name: node.name, phylesis: node.PHYLESIS, 
      extinct: node.EXTINCT, confidence: node.CONFIDENCE, childcount: node.CHILDCOUNT};
  console.log('node id ' + node['ID'] + ', node name ' + node.name);
  client.execute(gremlin_query, gremlin_bindings,
    (err, results) => {
      if (err) {
        lost_nodes[node_key] = node;
        console.error(err);
        async.nextTick(callback);
        return;
        //return callback();
      }
      console.log(JSON.stringify(results));
      // Save the node ID in an index table (used for loading the edges)
      node_index_table[node_key] = results[0]
      nb_nodes_being_written += 1
      if (nb_nodes_being_written % 1000 === 0){
        console.log('-----------------------------------------')
        console.log('Nodes written so far: ' + nb_nodes_being_written)
        console.log('-----------------------------------------')
      }
      async.nextTick(callback);
      return results[0];
      //
    }
  );
};


var timeout_writeNode = async.timeout(writeNode, 30000);

function wrapped_timeout_writeNode(node, node_key, callback){
  timeout_writeNode(node, node_key,
    (err,results) => {
      if (err) {
        console.error(err);
        timeout_node_list[node_key] = node;
        callback();
        return;
      }
      callback();
    });
}


function retry_missed_nodes(callback){
  var nb_lost_nodes = Object.keys(lost_nodes).length;
  if (nb_lost_nodes > 0){
    console.log('Retrying to load ' + nb_lost_nodes + ' missed nodes...')
    var lost_nodes_tmp = lost_nodes;
    lost_nodes = {};
    writeTheNodes(lost_nodes_tmp,callback);
  }
  callback();
}


function endNodeProcess(callback){
  console.log('Node written.')
  var nodes_not_loaded = Object.keys(node_data).length - Object.keys(node_index_table).length;
  if (nodes_not_loaded > 0){
    console.log('Warning, ' + nodes_not_loaded + ' nodes have not been loaded to the database.')
  }
  callback();
}



////////////////////////////////////////////////////////////////////////////////
// Functions to save the edges
////////////////////////////////////////////////////////////////////////////////

function get_edge_data(callback){
  callback(null,edge_data);
}

function writeTheEdges(edge_data, callback){
  console.log('')
  console.log('Start writing edges to the database.')
  async.eachOfLimit(edge_data,40, wrapped_timeout_writeEdge, function(err, result){
    // if any of the saves produced an error, err would equal that error
    if (err) {
      console.error(err);
      callback();
      return;
    }
    var time_step = new Date().getTime()
    var diff_time2 = time_step - start_time
    console.log("Time spent so far: " + diff_time2 + "ms or " + (diff_time2/1000/60) + " min.");
    callback();
  });
}

var lost_edges = {};
var nb_edges_being_written = 0;
var writeEdge = function(edge, edge_key, callback){
  if ((edge.source in node_index_table) && (edge.target in node_index_table)){ // if the nodes exist, create the edges
    var source_idx = node_index_table[edge.source]
    var target_idx = node_index_table[edge.target] 
    client.execute("g.V(source).addE('descendant').to(g.V(target))", { source: source_idx, target: target_idx},
      (err, results) => {
        if (err) {
          lost_edges[edge_key] = edge;
          console.error(err);
          async.nextTick(callback);
          return;
          //return callback();
        }
        //console.log(JSON.stringify(results));
        console.log('Edge ' + edge_key + ' with source node '+ source_idx+ ' and target node '+ target_idx + ' written.')
        nb_edges_being_written += 1
        if (nb_edges_being_written % 1000 === 0){
          console.log('-----------------------------------------')
          console.log('Edges written so far: ' + nb_edges_being_written)
          console.log('-----------------------------------------')
        }
        async.nextTick(callback);
        return;
        //
      }
    );
  } else {
    console.log('Warning, node missing for the creation of edge (' + edge.source + ',' + edge.target+ ')')
    async.nextTick(callback);
  }
};



var timeout_writeEdge = async.timeout(writeEdge, 30000);
var timeout_edge_list = {}

function wrapped_timeout_writeEdge(edge, edge_key, callback){
  timeout_writeEdge(edge, edge_key,
    (err,results) => {
      if (err) {
        console.error(err);
        timeout_edge_list[edge_key] = edge;
        callback();
        return;
      }

      callback();
    });
}

function retry_missed_edges(callback){
  var nb_edges_lost = Object.keys(lost_edges).length;
  if (nb_edges_lost > 0){
    console.log('Retrying to load ' + nb_edges_lost + ' missed edges...')
    var lost_edges_tmp = lost_edges;
    lost_edges = {};
    writeTheEdges(lost_edges_tmp, callback);
  }
  callback();
}


////////////////////////////////////////////////////////////////////////////////
// Utils
////////////////////////////////////////////////////////////////////////////////

// Clean the  database
function eraseDB(callback){
  console.log('Erasing the database...');
  client.execute('g.V().drop()', { }, (err, results) => {
    if (err) return console.error(err);
    console.log( 'Database erased ' + results);
    var diff_time1 = (new Date().getTime() - start_time)
    console.log("Time spent to erase the DB: " + diff_time1);
    console.log();
    callback();
  });
}


function query_nb_nodes_edges(){
  var gremlin_query = "g.V().count()";
  client.execute(gremlin_query,
    {},
    (err, results) => {
      if (err) return console.error(err);
      var nb_nodes = JSON.stringify(results);
      console.log('-----------------------------------------')
      console.log('Number of nodes in database: ' + nb_nodes);
      console.log('-----------------------------------------')
      return nb_nodes;
      //
    }
  );
  var gremlin_query = "g.E().count()";
  client.execute(gremlin_query,
    {},
    (err, results) => {
      if (err) return console.error(err);
      var nb_edges = JSON.stringify(results);
      console.log('-----------------------------------------')
      console.log('Number of edges in database: ' + nb_edges);
      console.log('-----------------------------------------')
      return nb_edges;
      //
    }
  );
}
