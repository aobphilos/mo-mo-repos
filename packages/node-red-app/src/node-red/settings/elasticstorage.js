/**
 * Copyright 2014 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
const {
  Client
} = require('@elastic/elasticsearch')
var rabbitmq = require('../../messagelogic/src/components/RabbitMQManager').RabbitMQManager
const ApiProxy = require('../../messagelogic/src/components/ApiProxy').ApiProxy
const INDEX = process.env.FLOW_INDEX || ("njoin-" + process.env.nuser)
let TYPE
// setTimeout(()=>{
//      require("../flow").ensureSearchTerm();
// }, 5000);
var when = require('when')
var util = require('util')

var appname
var readonly = process.env.demo ? true : false
let client
const ELASTICSEARCH_ENDPOINT = process.env.DBRVP_HOST ?
  (process.env.DBRVP_HOST + ':' + (process.env.DBRVP_PORT || '2080')) : process.env.nes
const createDummyFlowIfNotExist = function (resolve) {
  client.search({
    index: INDEX + '-' + TYPE.toLowerCase() + "-aliases",
    size: 0,
    body: {
      query: {
        match_all: {}
      }
    }
  }, function (err, resp) {
    if (err || !resp.body.hits) {
      console.error("error ", err, " while querying type ", TYPE, " resp is ", resp)
      if (resp.body.type == "index_not_found_exception" || resp.body.hits.total.value == 0) {
        console.log("Flow is empty, inserting empty flow object")
        saveFlows([], 100).then(resolve)
      }
      resolve()
    } else {
      if (resp.body.hits.total.value == 0) {
        console.log("Flow is empty, inserting empty flow object")
        saveFlows([], 100).then(resolve)
      } else resolve()
    }
  })
}

const createIndex = async () =>{
  console.log("creating index INDEX" + TYPE.toLowerCase() + "-credentials")
  return client.indices.create({ index: INDEX + '-' + TYPE.toLowerCase() + "-credentials", });
}


function init() {
  return when.promise(function (resolve, reject, notify) {
    client = new Client({
      node: ELASTICSEARCH_ENDPOINT,
      auth: {
        username: process.env.nuser,
        password: process.env.npwd
      },
      requestTimeout: 90000
    })
    console.log("Flow storage: Elasticsearch client created with index ", INDEX, " and type " + TYPE)
    const ts = new Date().getTime()

    client.update({
      index: INDEX + '-activity_log-aliases',
      id: "A_" + ts + "_" + Math.floor(Math.random() * 10000),
      retryOnConflict: 3,
      body: {
        "doc": {
          log_timestamp: ts,
          type: TYPE,
          activity: "START"
        },
        "doc_as_upsert": true
      }
    }, (err, response) => {
      if (err) {
        console.error("Error while initializing elasticsearch schema " + err)
        reject(err)
      } else
        createDummyFlowIfNotExist(resolve)
    })
  })
}

function timeoutWrap(func) {
  return when.promise(function (resolve, reject, notify) {
    var promise = func().timeout(10000, "timeout")
    promise.then(function (a, b, c, d) {
      //heartBeatLastSent = (new Date()).getTime();
      resolve(a, b, c, d)
    })
    promise.otherwise(function (err) {
      console.log("TIMEOUT: ", func.name)
      reject(err)
    })
  })
}

function getFlows() {
  var defer = when.defer()
  const body = {
    "size": 1,
    "query": {
      "match_all": {}
    },
    "sort": {
      "deploy_timestamp": "desc"
    }
  }
  client.search({
    index: INDEX + '-' + TYPE.toLowerCase() + "-aliases",
    size: 1,
    body: body
  }).then((resp) => {
    if (!resp || !resp.body.hits) {
      console.error("error while querying system flow with body ", body, " resp is ", resp)
      defer.reject(new Error("Flow error " + resp))
    } else if (!resp.body.hits.hits || resp.body.hits.hits.length == 0) {
      defer.resolve([])
    } else {
      defer.resolve(JSON.parse(resp.body.hits.hits[0]._source.flow))
    }
  })
  return defer.promise
}

function saveFlows(flow, deployts) {
  const defer = when.defer()

  const ts = deployts ? Number(deployts) : new Date().getTime()
  flow = {
    flow: JSON.stringify(flow),
    deploy_timestamp: ts
  }
  const id = ts + ""
  console.log("Inserting flow id  ", id, " to elasticsearch")
  client.update({
    index: INDEX + "-activity_log-aliases",
    id: "A_" + ts + "_" + Math.floor(Math.random() * 10000),
    retryOnConflict: 3,
    body: {
      "doc": {
        log_timestamp: ts,
        type: TYPE,
        activity: "DEPLOY"
      },
      "doc_as_upsert": true
    }
  }, (err, response) => {
    if (err) {
      console.error("Error while writing activity log to elasticsearch " + err)
    }
  })
  client.update({
    index: INDEX + '-' + TYPE.toLowerCase() + "-aliases",
    id: id,
    refresh: 'true',
    body: {
      "doc": flow,
      "doc_as_upsert": true
    }
  }, (err, response) => {
    if (err) {
      console.error("Error while inserting flow ", id, " to elasticsearch ", err)
      defer.reject(err)
    } else {
      if (process.env.USE_MQ) {
        rabbitmq.getInstance().init().then(() => {
          console.debug('channel re-init')
          rabbitmq.getInstance().sendBroadcastDeploy()
        })
      }
      ApiProxy.getInstance().sendReqest('PUT', '/notification/origin', {
          message: "Messagelogic has been deployed!",
          link: "#/messagelogic",
          icon: "checkmark",
          color: "success",
          timestamp: ts,
          should_refresh: false
        })
        .then(() => {
          console.log('Sent deploy notification from messagelogic to api')
        })
        .catch((err) => {
          console.error('Error while sending deploy notification from messagelogic to api :', err)
        })
      defer.resolve()
    }
  })
  // if(readonly){
  //     console.log("Flow not saved for readonly instance");
  //     return defer.promise;
  // }
  // collection().then(function(collection) {
  //     collection.update({appname:appname},{$set:{appname:appname,flow:flows}},{upsert:true},function(err) {
  //         if (err) {
  //             defer.reject(err);
  //         } else {
  //             defer.resolve();
  //         }
  //     })
  // }).otherwise(function(err) {
  //     defer.reject(err);
  // });
  return defer.promise
}

function getCredentials() {
  var defer = when.defer()
  // collection().then(function(collection) {
  //     collection.findOne({appname:appname},function(err,doc) {
  //         if (err) {
  //             defer.reject(err);
  //         } else {
  //             if (doc && doc.credentials) {
  //                 var credentials = decodeJson(doc.credentials);
  //                 defer.resolve(credentials);
  //             } else {
  //                 defer.resolve({});
  //             }
  //         }
  //     })
  // }).otherwise(function(err) {
  //     defer.reject(err);
  // });

  const body = {
    "size": 1,
    "query": {
      "match_all": {
            }
    },
        "sort": [{
      "deploy_timestamp": {
                "order": "desc"
            }
    }]
  }
  client.search({
    index: INDEX + '-' + TYPE.toLowerCase() + "-credentials-aliases",
    size: 1,
    body: body
  }).then((resp) => {
    if (!resp || !resp.body.hits) {
      console.error("error while querying system flow with body ", body, " resp is ", resp)
      defer.reject(new Error("Flow error " + resp))
    } else if (!resp.body.hits.hits || resp.body.hits.total.value == 0) {
      defer.resolve({})
    } else {
      defer.resolve(JSON.parse(resp.body.hits.hits[0]._source.flow))
    }
  }).catch(async(err) => {
      if (err && err.meta && err.meta.body && err.meta.body.error && err.meta.body.error.type == "index_not_found_exception") {
        await createIndex();
        defer.resolve({})
      } else {
        console.error("error ", err, " while querying system flow with body ")
        defer.reject(new Error("Flow error " + err))
      }
    }

  )
  return defer.promise
}

function saveCredentials(credentials) {
  var defer = when.defer()
  const ts = new Date().getTime()
  credentials = {
    flow: JSON.stringify(credentials),
    deploy_timestamp: ts
  }
  const id = ts + ""
  console.log("Inserting flow id  ", id, " to elasticsearch")
  client.update({
    index: INDEX + '-' + TYPE.toLowerCase() + "-credentials-aliases",
    id: id,
    body: {
      "doc": credentials,
      "doc_as_upsert": true
    }
  }, (err, response) => {
    if (err) {
      console.error("Error while inserting flow ", id, " to elasticsearch ", err)
      defer.reject(err)
    } else
      defer.resolve()
  })
  // if(readonly){
  //     console.log("Flow not saved for readonly instance");
  //     return defer.promise;
  // }
  // collection().then(function(collection) {
  //     collection.update({appname:appname},{$set:{appname:appname,flow:flows}},{upsert:true},function(err) {
  //         if (err) {
  //             defer.reject(err);
  //         } else {
  //             defer.resolve();
  //         }
  //     })
  // }).otherwise(function(err) {
  //     defer.reject(err);
  // });
  return defer.promise
}

function getLibraryEntry(type, path) {
  var defer = when.defer()
  client.get({
    index: INDEX + '-' + TYPE.toLowerCase() + "-library-aliases",
    id: type + "-p" + path
  }, function (err, response) {
    if (err) {
      // console.error("Error while getting library "+(type+"-"+path)+" of type ", TYPE, " from elasticsearch ", err, " response ", response);
      defer.resolve([])
    } else
      resolve(JSON.parse(response.body._source.data))
  })
  // libCollection().then(function(libCollection) {
  //     libCollection.findOne({appname:appname, type:type, path:path}, function(err,doc) {
  //         if (err) {
  //             defer.reject(err);
  //         } else if (doc) {
  //             defer.resolve(doc.data);
  //         } else {
  //             if (path != "" && path.substr(-1) != "/") {
  //                 path = path+"/";
  //             }
  //             libCollection.find({appname:appname, type:type, path:{$regex:path+".*"}},{sort:'path'}).toArray(function(err,docs) {
  //                 if (err) {
  //                     defer.reject(err);
  //                 } else if (!docs) {
  //                     defer.reject("not found");
  //                 } else {
  //                     var dirs = [];
  //                     var files = [];
  //                     for (var i=0;i<docs.length;i++) {
  //                         var doc = docs[i];
  //                         var subpath = doc.path.substr(path.length);
  //                         var parts = subpath.split("/");
  //                         if (parts.length == 1) {
  //                             var meta = doc.meta;
  //                             meta.fn = parts[0];
  //                             files.push(meta);
  //                         } else if (dirs.indexOf(parts[0]) == -1) {
  //                             dirs.push(parts[0]);
  //                         }
  //                     }
  //                     defer.resolve(dirs.concat(files));
  //                 }
  //             });
  //         }
  //     });
  // }).otherwise(function(err) {
  //     defer.reject(err);
  // });
  return defer.promise
}

function saveLibraryEntry(type, path, meta, body) {
  var defer = when.defer()

  const ts = new Date().getTime()
  const flow = {
    flow: JSON.stringify(body),
    deploy_timestamp: ts
  }
  const id = ts + ""
  client.update({
    index: INDEX + '-' + TYPE.toLowerCase() + "-library-aliases",
    id: type + "-p" + path,
    body: {
      "doc": {
        data: JSON.stringify({
          meta: meta,
          data: flow
        })
      },
      "doc_as_upsert": true
    }
  }, (err, response) => {
    if (err) {
      console.error("Error while inserting flow ", id, " to elasticsearch ", err)
      defer.reject(err)
    } else
      defer.resolve()
  })
  // if(readonly){
  //     console.log("Library not saved for readonly instance");
  //     return defer.promise;
  // }
  // libCollection().then(function(libCollection) {
  //     libCollection.update({appname:appname,type:type,path:path},{appname:appname,type:type,path:path,meta:meta,data:body},{upsert:true},function(err) {
  //         if (err) {
  //             defer.reject(err);
  //         } else {
  //             defer.resolve();
  //         }
  //     });
  // }).otherwise(function(err) {
  //     defer.reject(err);
  // });
  return defer.promise
}
let settings
var mongostorage = {
  init: function (_settings) {
    settings = _settings
    TYPE = "flow"
    if (settings.flowType && settings.flowType.length > 0) TYPE += "-" + settings.flowType
    return init()
  },
  getFlows: function () {
    return timeoutWrap(getFlows)
  },
  saveFlows: function (flows) {
    return timeoutWrap(function () {
      return saveFlows(flows)
    })
  },

  getCredentials: function () {
    return timeoutWrap(getCredentials)
  },

  saveCredentials: function (credentials) {
    return timeoutWrap(function () {
      return saveCredentials(credentials)
    })
  },
  getLibraryEntry: function (type, path) {
    return timeoutWrap(function () {
      return getLibraryEntry(type, path)
    })
  },
  saveLibraryEntry: function (type, path, meta, body) {
    return timeoutWrap(function () {
      return saveLibraryEntry(type, path, meta, body)
    })
  }
}

module.exports = mongostorage
