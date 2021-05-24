import { QueryPQLRequest, QuerySQLRequest } from 'proto/pilosa_pb';
import { grpc } from '@improbable-eng/grpc-web';
import { Pilosa } from 'proto/pilosa_pb_service';
import { baseURL as host } from './baseURL';

const invokeStream = (operation, request, onMessage, onEnd) => {
  grpc.invoke(operation, {
    host,
    request,
    onMessage: (message: grpc.ProtobufMessage) => onMessage(message),
    onEnd: (status: grpc.Code, statusMessage: string) => {
      onEnd(status, statusMessage);
    }
  });
};

export const queryPQL = (index: string, pql: string, onMessage, onEnd) => {
  const queryPQLRequest = new QueryPQLRequest();
  queryPQLRequest.setIndex(index);
  queryPQLRequest.setPql(pql);
  invokeStream(Pilosa.QueryPQL, queryPQLRequest, onMessage, onEnd);
};

export const querySQL = (sql: string, onMessage, onEnd) => {
  const querySQLRequest = new QuerySQLRequest();
  querySQLRequest.setSql(sql);
  invokeStream(Pilosa.QuerySQL, querySQLRequest, onMessage, onEnd);
}
