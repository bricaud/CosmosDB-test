"use strict";


var config = require("./config");
var fs = require('fs');
var async = require('async');


var json_filename = '';
var gremlin_client = null;

// Global variable for the loading the nodes
var node_data;
var node_index_table = {};
var nb_nodes_being_written = 0;
var lost_nodes = {};
var timeout_node_list = {};


// Global variables for loading the edges
var edge_data;

var nb_edges_being_written = 0;
var lost_edges = {};
var timeout_edge_list = {}


// Global variables for meauring the time spent for loading data
var now = new Date();
var start_time = now.getTime();
var node_inter_time = now.getTime();
var edge_inter_time = now.getTime();
var time_since_start = 0




// Main function: read data from the file and write the the database
function loadjson(filename,client){
  json_filename = filename;
  gremlin_client = client;
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
      //console.log(lost_nodes,lost_edges,timeout_node_list,timeout_edge_list);
    }
  );
}

// Function to read the data from file
function readDataFile(callback) {
  fs.readFile(json_filename, 'utf8', function (err, data) {
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
    var process_limit = 40;
    callback(null, node_data, process_limit);

    // A command closing the connection is missing, like:
    //client.closeConnection();

  });
}


function initProcess(callback){
  // Measure the time to load the data
  start_time = now.getTime();
  callback();
}

function endProcess(callback){
  console.log('Nodes and edges written.')
  regular_info();
  //setInterval(regular_info,30000)
  // Check lost nodes
  //console.log('Number of lost nodes: ' + Object.keys(lost_nodes).length)
  callback(null,lost_nodes,lost_edges,timeout_node_list,timeout_edge_list);
}



function regular_info(){
  console.log('------------------------------------')
  console.log('Node info')
  console.log('------------------------------------')
  var nb_nodes_to_write = Object.keys(node_data).length;
  console.log('Nb of nodes found in file: ' + nb_nodes_to_write)
  var nb_nodes_written = Object.keys(node_index_table).length;
  console.log('Nb of nodes written (not counting timed out): '+ nb_nodes_written);
  var nb_nodes_missed = Object.keys(lost_nodes).length;
  console.log('Nb of nodes lost: '+ nb_nodes_missed);
  var nb_nodes_timed_out = Object.keys(timeout_node_list).length;
  console.log('Nb of node requests that timed out: ' + nb_nodes_timed_out )
  console.log('written + lost + timed_out: ' + (nb_nodes_written + nb_nodes_missed + nb_nodes_timed_out) )
  console.log('Time spent for writing nodes: ' + (node_inter_time/1000/60) + 'min')
  console.log('------------------------------------')
  console.log('Edge info')
  console.log('------------------------------------')
  var nb_edges_to_write = Object.keys(edge_data).length;
  console.log('Nb of edges found in file: ' + nb_edges_to_write)
  console.log('Nb of edges written (not counting timed out): '+ nb_edges_being_written);
  var nb_edges_missed = Object.keys(lost_edges).length;
  console.log('Nb of edges lost: '+ nb_edges_missed);
  var nb_edges_timed_out = Object.keys(timeout_edge_list).length;
  console.log('Nb of edge requests that timed out: ' + nb_edges_timed_out )  
  console.log('written + lost + timed_out: ' + (nb_edges_being_written + nb_edges_missed + nb_edges_timed_out) )
  console.log('Time spent for writing edges: ' + ((edge_inter_time - node_inter_time)/1000/60) + 'min')
  console.log('------------------------------------')
  console.log('Total time spent for writing nodes and edges: ' + (edge_inter_time/1000/60) + 'min')
  console.log('------------------------------------')
  console.log('WARNINGS')
  console.log('------------------------------------')
  if ((nb_nodes_timed_out + nb_edges_timed_out) >0){
    console.log('Some queries timed out during the loading. Please check if it eventually succeeded.')
  } else {
    console.log('No warning.')
  }
  // Query the number of nodes and edges (asynchroneous!)  
  //var nodes_in_database = query_nb_nodes_edges();
  //console.log('Time since started: ' + time_since_start + ' seconds, or ' + (time_since_start/60) + 'min')
  //time_since_start += 30
}


////////////////////////////////////////////////////////////////////////////////
// Functions to save the node
////////////////////////////////////////////////////////////////////////////////



function writeTheNodes(node_data, process_limit, callback){
  console.log('')
  console.log('Start writing nodes to the database.')
  async.eachOfLimit(node_data,process_limit, wrapped_timeout_writeNode, function(err, result){
    // if any of the saves produced an error, err would equal that error
    if (err) {
      console.error(err);
      callback();
      return;
    }
    var time_step = new Date().getTime();
    node_inter_time = (time_step - start_time)
    console.log("Time spent so far: " + node_inter_time + "ms or " + (node_inter_time/1000/60) + " min.");
    callback();
  });
}

// Add a timeout to the node function
function wrapped_timeout_writeNode(object, object_key, callback){
  wrapped_timeout_writeObject(object, object_key, writeSingleNode, timeout_node_list, callback);
}


function writeSingleNode(node, node_key, callback){
  if ('label' in node){
    var gremlin_query = "g.addV('" + node['label'] + "')";
  } else {
    console.log('Warning: node has no label. Giving default label "label1".')
    var gremlin_query = "g.addV('label1')";
  }
  var gremlin_bindings = {};
  for (var prop_key in node){
    if( node.hasOwnProperty( prop_key ) && prop_key != 'id' && prop_key != 'label') {
      gremlin_query = gremlin_query + ".property('" + prop_key + "'," + prop_key + ")";
      gremlin_bindings[prop_key] = node[prop_key];
    }
  }
  gremlin_query = gremlin_query + '.id()'
  //console.log(gremlin_query)
  //console.log(gremlin_bindings)
  /*
  var gremlin_query = "g.addV('species').property('node_id', node_id).property('name', name)" +
      ".property('phylesis', phylesis).property('extinct', extinct).property('confidence', confidence)" +
      ".property('childcount', childcount).id()";
  var gremlin_bindings = { 'node_id': node['ID'], 'name': node.name, 'phylesis': node.PHYLESIS, 
      'extinct': node.EXTINCT, 'confidence': node.CONFIDENCE, 'childcount': node.CHILDCOUNT};
  */
  console.log('Node nb: ' + node_key + ', node id ' + node['ID'] + ', node name ' + node.name);
  gremlin_client.execute(gremlin_query, gremlin_bindings,
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
      async.nextTick(callback);
      return results[0];
      //
    }
  );
};


function retry_missed_nodes(callback){
  retry_missed_objects('nodes', lost_nodes, writeTheNodes, callback);
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
  var process_limit = 40;
  callback(null,edge_data,process_limit);
}

function writeTheEdges(edge_data, process_limit, callback){
  console.log('')
  console.log('Start writing edges to the database.')
  async.eachOfLimit(edge_data, process_limit, wrapped_timeout_writeEdge, function(err, result){
    // if any of the saves produced an error, err would equal that error
    if (err) {
      console.error(err);
      callback();
      return;
    }
    var time_step = new Date().getTime()
    edge_inter_time = time_step - start_time
    console.log("Time spent so far: " + edge_inter_time + "ms or " + (edge_inter_time/1000/60) + " min.");
    callback();
  });
}

var writeSingleEdge = function(edge, edge_key, callback){
  if ((edge.source in node_index_table) && (edge.target in node_index_table)){ // if the nodes exist, create the edges
    var source_idx = node_index_table[edge.source]
    var target_idx = node_index_table[edge.target]
    // Create the query
    if ('label' in edge){
      var gremlin_query = "g.V(source).addE('" + edge['label'] + "').to(g.V(target))";
    } else {
      console.log('Warning: edge has no label. Giving default label "edge_label1".')
      var gremlin_query = "g.V(source).addE('edge_label1').to(g.V(target))";
    }
    var gremlin_bindings = {};
    gremlin_bindings['source'] = source_idx;
    gremlin_bindings['target'] = target_idx;
    for (var prop_key in edge){
      if( edge.hasOwnProperty( prop_key ) && 
        prop_key != 'id' && prop_key != 'label' &&
        prop_key != 'source' && prop_key != 'target') {
        gremlin_query = gremlin_query + ".property('" + prop_key + "'," + prop_key + ")";
        gremlin_bindings[prop_key] = edge[prop_key];
      }
    }
    gremlin_query = gremlin_query + '.id()'

    // Send the query
    gremlin_client.execute(gremlin_query, gremlin_bindings,
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

// Add a timeout to the edge function
function wrapped_timeout_writeEdge(edge, edge_key, callback){
  wrapped_timeout_writeObject(edge,edge_key,writeSingleEdge,timeout_edge_list,callback);
}

function retry_missed_edges(callback){
  retry_missed_objects('edges', lost_edges, writeTheEdges, callback);
}




////////////////////////////////////////////////////////////////////////////////
// Common functions
////////////////////////////////////////////////////////////////////////////////

// Add a timeout to the async function 'objectFunction' 
// and record the object for which there was a timeout in 'timeout_list'
function wrapped_timeout_writeObject(object, object_key, objectFunction, timeout_list, callback){
  var timeout_writeObject = async.timeout(objectFunction, 30000);
  timeout_writeObject(object, object_key,
    (err,results) => {
      if (err) {
        console.error(err);
        timeout_list[object_key] = object;
        callback();
        return;
      }
      callback();
    });
}


// Retry to load the data from the list 'lost_objects' using 'writeFunction'
function retry_missed_objects(type, lost_objects, writeFunction, callback){
  var nb_lost_objects = Object.keys(lost_objects).length;
  if (nb_lost_objects > 0){
    console.log('Retrying to load ' + nb_lost_objects + ' missed ' + type + '...')
    var lost_objects_tmp = lost_objects;
    lost_objects = {};
    var process_limit = 10;
    writeFunction(lost_objects_tmp, process_limit, callback);
  } else callback(); 
}

////////////////////////////////////////////////////////////////////////////////
// Utils
////////////////////////////////////////////////////////////////////////////////

// Clean the  database
function eraseDB(callback){
  console.log('Erasing the database...');
  gremlin_client.execute('g.V().drop()', { }, (err, results) => {
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
  gremlin_client.execute(gremlin_query,
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
  gremlin_client.execute(gremlin_query,
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

exports.loadjson = loadjson;
exports.query_nb_nodes_edges = query_nb_nodes_edges;