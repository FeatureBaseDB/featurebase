/* eslint-disable */
// package: pilosa
// file: pilosa.proto

var pilosa_pb = require("./pilosa_pb");
var grpc = require("@improbable-eng/grpc-web").grpc;

var Pilosa = (function () {
  function Pilosa() {}
  Pilosa.serviceName = "proto.Pilosa";
  return Pilosa;
}());

Pilosa.CreateIndex = {
  methodName: "CreateIndex",
  service: Pilosa,
  requestStream: false,
  responseStream: false,
  requestType: pilosa_pb.CreateIndexRequest,
  responseType: pilosa_pb.CreateIndexResponse
};

Pilosa.GetIndexes = {
  methodName: "GetIndexes",
  service: Pilosa,
  requestStream: false,
  responseStream: false,
  requestType: pilosa_pb.GetIndexesRequest,
  responseType: pilosa_pb.GetIndexesResponse
};

Pilosa.GetIndex = {
  methodName: "GetIndex",
  service: Pilosa,
  requestStream: false,
  responseStream: false,
  requestType: pilosa_pb.GetIndexRequest,
  responseType: pilosa_pb.GetIndexResponse
};

Pilosa.DeleteIndex = {
  methodName: "DeleteIndex",
  service: Pilosa,
  requestStream: false,
  responseStream: false,
  requestType: pilosa_pb.DeleteIndexRequest,
  responseType: pilosa_pb.DeleteIndexResponse
};

Pilosa.QuerySQL = {
  methodName: "QuerySQL",
  service: Pilosa,
  requestStream: false,
  responseStream: true,
  requestType: pilosa_pb.QuerySQLRequest,
  responseType: pilosa_pb.RowResponse
};

Pilosa.QuerySQLUnary = {
  methodName: "QuerySQLUnary",
  service: Pilosa,
  requestStream: false,
  responseStream: false,
  requestType: pilosa_pb.QuerySQLRequest,
  responseType: pilosa_pb.TableResponse
};

Pilosa.QueryPQL = {
  methodName: "QueryPQL",
  service: Pilosa,
  requestStream: false,
  responseStream: true,
  requestType: pilosa_pb.QueryPQLRequest,
  responseType: pilosa_pb.RowResponse
};

Pilosa.QueryPQLUnary = {
  methodName: "QueryPQLUnary",
  service: Pilosa,
  requestStream: false,
  responseStream: false,
  requestType: pilosa_pb.QueryPQLRequest,
  responseType: pilosa_pb.TableResponse
};

Pilosa.Inspect = {
  methodName: "Inspect",
  service: Pilosa,
  requestStream: false,
  responseStream: true,
  requestType: pilosa_pb.InspectRequest,
  responseType: pilosa_pb.RowResponse
};

exports.Pilosa = Pilosa;

function PilosaClient(serviceHost, options) {
  this.serviceHost = serviceHost;
  this.options = options || {};
}

PilosaClient.prototype.createIndex = function createIndex(requestMessage, metadata, callback) {
  if (arguments.length === 2) {
    callback = arguments[1];
  }
  var client = grpc.unary(Pilosa.CreateIndex, {
    request: requestMessage,
    host: this.serviceHost,
    metadata: metadata,
    transport: this.options.transport,
    debug: this.options.debug,
    onEnd: function (response) {
      if (callback) {
        if (response.status !== grpc.Code.OK) {
          var err = new Error(response.statusMessage);
          err.code = response.status;
          err.metadata = response.trailers;
          callback(err, null);
        } else {
          callback(null, response.message);
        }
      }
    }
  });
  return {
    cancel: function () {
      callback = null;
      client.close();
    }
  };
};

PilosaClient.prototype.getIndexes = function getIndexes(requestMessage, metadata, callback) {
  if (arguments.length === 2) {
    callback = arguments[1];
  }
  var client = grpc.unary(Pilosa.GetIndexes, {
    request: requestMessage,
    host: this.serviceHost,
    metadata: metadata,
    transport: this.options.transport,
    debug: this.options.debug,
    onEnd: function (response) {
      if (callback) {
        if (response.status !== grpc.Code.OK) {
          var err = new Error(response.statusMessage);
          err.code = response.status;
          err.metadata = response.trailers;
          callback(err, null);
        } else {
          callback(null, response.message);
        }
      }
    }
  });
  return {
    cancel: function () {
      callback = null;
      client.close();
    }
  };
};

PilosaClient.prototype.getIndex = function getIndex(requestMessage, metadata, callback) {
  if (arguments.length === 2) {
    callback = arguments[1];
  }
  var client = grpc.unary(Pilosa.GetIndex, {
    request: requestMessage,
    host: this.serviceHost,
    metadata: metadata,
    transport: this.options.transport,
    debug: this.options.debug,
    onEnd: function (response) {
      if (callback) {
        if (response.status !== grpc.Code.OK) {
          var err = new Error(response.statusMessage);
          err.code = response.status;
          err.metadata = response.trailers;
          callback(err, null);
        } else {
          callback(null, response.message);
        }
      }
    }
  });
  return {
    cancel: function () {
      callback = null;
      client.close();
    }
  };
};

PilosaClient.prototype.deleteIndex = function deleteIndex(requestMessage, metadata, callback) {
  if (arguments.length === 2) {
    callback = arguments[1];
  }
  var client = grpc.unary(Pilosa.DeleteIndex, {
    request: requestMessage,
    host: this.serviceHost,
    metadata: metadata,
    transport: this.options.transport,
    debug: this.options.debug,
    onEnd: function (response) {
      if (callback) {
        if (response.status !== grpc.Code.OK) {
          var err = new Error(response.statusMessage);
          err.code = response.status;
          err.metadata = response.trailers;
          callback(err, null);
        } else {
          callback(null, response.message);
        }
      }
    }
  });
  return {
    cancel: function () {
      callback = null;
      client.close();
    }
  };
};

PilosaClient.prototype.querySQL = function querySQL(requestMessage, metadata) {
  var listeners = {
    data: [],
    end: [],
    status: []
  };
  var client = grpc.invoke(Pilosa.QuerySQL, {
    request: requestMessage,
    host: this.serviceHost,
    metadata: metadata,
    transport: this.options.transport,
    debug: this.options.debug,
    onMessage: function (responseMessage) {
      listeners.data.forEach(function (handler) {
        handler(responseMessage);
      });
    },
    onEnd: function (status, statusMessage, trailers) {
      listeners.status.forEach(function (handler) {
        handler({ code: status, details: statusMessage, metadata: trailers });
      });
      listeners.end.forEach(function (handler) {
        handler({ code: status, details: statusMessage, metadata: trailers });
      });
      listeners = null;
    }
  });
  return {
    on: function (type, handler) {
      listeners[type].push(handler);
      return this;
    },
    cancel: function () {
      listeners = null;
      client.close();
    }
  };
};

PilosaClient.prototype.querySQLUnary = function querySQLUnary(requestMessage, metadata, callback) {
  if (arguments.length === 2) {
    callback = arguments[1];
  }
  var client = grpc.unary(Pilosa.QuerySQLUnary, {
    request: requestMessage,
    host: this.serviceHost,
    metadata: metadata,
    transport: this.options.transport,
    debug: this.options.debug,
    onEnd: function (response) {
      if (callback) {
        if (response.status !== grpc.Code.OK) {
          var err = new Error(response.statusMessage);
          err.code = response.status;
          err.metadata = response.trailers;
          callback(err, null);
        } else {
          callback(null, response.message);
        }
      }
    }
  });
  return {
    cancel: function () {
      callback = null;
      client.close();
    }
  };
};

PilosaClient.prototype.queryPQL = function queryPQL(requestMessage, metadata) {
  var listeners = {
    data: [],
    end: [],
    status: []
  };
  var client = grpc.invoke(Pilosa.QueryPQL, {
    request: requestMessage,
    host: this.serviceHost,
    metadata: metadata,
    transport: this.options.transport,
    debug: this.options.debug,
    onMessage: function (responseMessage) {
      listeners.data.forEach(function (handler) {
        handler(responseMessage);
      });
    },
    onEnd: function (status, statusMessage, trailers) {
      listeners.status.forEach(function (handler) {
        handler({ code: status, details: statusMessage, metadata: trailers });
      });
      listeners.end.forEach(function (handler) {
        handler({ code: status, details: statusMessage, metadata: trailers });
      });
      listeners = null;
    }
  });
  return {
    on: function (type, handler) {
      listeners[type].push(handler);
      return this;
    },
    cancel: function () {
      listeners = null;
      client.close();
    }
  };
};

PilosaClient.prototype.queryPQLUnary = function queryPQLUnary(requestMessage, metadata, callback) {
  if (arguments.length === 2) {
    callback = arguments[1];
  }
  var client = grpc.unary(Pilosa.QueryPQLUnary, {
    request: requestMessage,
    host: this.serviceHost,
    metadata: metadata,
    transport: this.options.transport,
    debug: this.options.debug,
    onEnd: function (response) {
      if (callback) {
        if (response.status !== grpc.Code.OK) {
          var err = new Error(response.statusMessage);
          err.code = response.status;
          err.metadata = response.trailers;
          callback(err, null);
        } else {
          callback(null, response.message);
        }
      }
    }
  });
  return {
    cancel: function () {
      callback = null;
      client.close();
    }
  };
};

PilosaClient.prototype.inspect = function inspect(requestMessage, metadata) {
  var listeners = {
    data: [],
    end: [],
    status: []
  };
  var client = grpc.invoke(Pilosa.Inspect, {
    request: requestMessage,
    host: this.serviceHost,
    metadata: metadata,
    transport: this.options.transport,
    debug: this.options.debug,
    onMessage: function (responseMessage) {
      listeners.data.forEach(function (handler) {
        handler(responseMessage);
      });
    },
    onEnd: function (status, statusMessage, trailers) {
      listeners.status.forEach(function (handler) {
        handler({ code: status, details: statusMessage, metadata: trailers });
      });
      listeners.end.forEach(function (handler) {
        handler({ code: status, details: statusMessage, metadata: trailers });
      });
      listeners = null;
    }
  });
  return {
    on: function (type, handler) {
      listeners[type].push(handler);
      return this;
    },
    cancel: function () {
      listeners = null;
      client.close();
    }
  };
};

exports.PilosaClient = PilosaClient;

/* eslint-enable */
