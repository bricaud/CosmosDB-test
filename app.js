"use strict";

var Gremlin = require('gremlin');
var config = require("./config");
var fs = require('fs');

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

// Clean the  database
console.log('Erasing the database...');
client.execute('g.V().drop()', { }, (err, results) => {
  if (err) return console.error(err);
  console.log(results);
  console.log();
});


// Main function: read data from the file and write the the database
fs.readFile('treeoflife.json', 'utf8', function (err, data) {
  if (err) throw err;
  var obj = JSON.parse(data);
  node_data = obj['nodes'];
  edge_data = obj['links'];
  console.log('Examples of data structure:')
  console.log('For nodes:')
  console.log(node_data[0])
  console.log('For edges:')
  console.log(edge_data[0])
  // Measure the time to load the data
  console.time("Time_to_save_nodes");
  console.time("dbtotalsave");
  // Launch the iterative process
  // The nodes and edges are written sequentially
  console.log('')
  console.log('Start writing to the database.')
  processData(0,0);
  // A command closing the connection is missing, like:
  //client.closeConnection();
});



// Function to load the data to the graphDB
var processData = function(node_list_index,edge_list_index){
  // First step: Write the nodes
  var nb_nodes = 50; // Number of nodes to write, full data: node_data.length
  if( node_list_index < nb_nodes ) {
    //console.log('Process node ' + list_index);
    writeNode(node_data[node_list_index], node_list_index, edge_list_index)
  }
  // Second step: write the edges
  // The nodes must exist before writing the edges!
  else  { 
    if (nb_nodes == node_list_index){
      // Display the time spend to write nodes
      console.timeEnd("Time_to_save_nodes");
      node_list_index = node_list_index + 1 // to avoid calling timeEnd multiple times
    }
    if (edge_list_index < edge_data.length) {
      writeEdge(edge_data[edge_list_index], node_list_index, edge_list_index)
    }
    else {
      console.log('Loading done. '+ (node_list_index-1) + ' nodes written.')
      console.timeEnd("dbtotalsave");
    }
  }
};



// Function to save the node
var node_index_table = {}
var writeNode = function(node,node_list_index,edge_list_index){
  var gremlin_query = "g.addV('species').property('node_id', node_id).property('name', name)" +
      ".property('phylesis', phylesis).property('extinct', extinct).property('confidence', confidence)" +
      ".property('childcount', childcount).id()";
  console.log('node id ' + node['ID'] + ', node name ' + node.name);
  client.execute(gremlin_query,
    { node_id: node['ID'], name: node.name, phylesis: node.PHYLESIS, 
      extinct: node.EXTINCT, confidence: node.CONFIDENCE, childcount: node.CHILDCOUNT},
    (err, results) => {
      if (err) return console.error(err);
      //console.log(JSON.stringify(results));
      // Save the node ID in an index table (used for loading the edges)
      node_index_table[node['ID']] = results[0]
      //
      handleStack(node_list_index+1,edge_list_index,processData)
      //processNodes(list_index+1);
    }
  );
};



var writeEdge = function(edge,node_list_index,edge_list_index){
  if ((edge.source in node_index_table) &&(edge.target in node_index_table)){ // if the nodes exist, create the edges
    var source_idx = node_index_table[edge.source]
    var target_idx = node_index_table[edge.target] 
    client.execute("g.V(source).addE('descendant').to(g.V(target))", { source: source_idx, target: target_idx}, (err, results) => {
      if (err) return console.error(err);
      //console.log(JSON.stringify(results));
      console.log('Edge '+ edge_list_index + ' with source node '+ source_idx+ ' and target node '+ target_idx + ' written.')
      handleStack(node_list_index,edge_list_index+1,processData)
    });
  } else {
    //console.log('Warning, Node missing for the creation of edge number' + list_index)
    handleStack(node_list_index, edge_list_index+1,processData)
  }
};



// This function is necessary to avoid stack overflow during the writing.
var handleStack = function(node_index, edge_index, function_to_process){
  // Call function_to_process
  // but:
  // Every 1000 steps, clear the stack
  
  //console.log(sumidx) 
  if( (node_index+edge_index) % 1000 === 0 ) {
    //console.log("Node " + node_index + ", edge " + edge_index)
    setTimeout(function(){function_to_process(node_index, edge_index);},0); // this allows to clear the stack before overflow
  } else {
    function_to_process(node_index, edge_index);
  }
}
