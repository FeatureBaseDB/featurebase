// package: pilosa
// file: pilosa.proto

import * as pilosa_pb from "./pilosa_pb";
import {grpc} from "@improbable-eng/grpc-web";

type PilosaCreateIndex = {
  readonly methodName: string;
  readonly service: typeof Pilosa;
  readonly requestStream: false;
  readonly responseStream: false;
  readonly requestType: typeof pilosa_pb.CreateIndexRequest;
  readonly responseType: typeof pilosa_pb.CreateIndexResponse;
};

type PilosaGetIndexes = {
  readonly methodName: string;
  readonly service: typeof Pilosa;
  readonly requestStream: false;
  readonly responseStream: false;
  readonly requestType: typeof pilosa_pb.GetIndexesRequest;
  readonly responseType: typeof pilosa_pb.GetIndexesResponse;
};

type PilosaGetIndex = {
  readonly methodName: string;
  readonly service: typeof Pilosa;
  readonly requestStream: false;
  readonly responseStream: false;
  readonly requestType: typeof pilosa_pb.GetIndexRequest;
  readonly responseType: typeof pilosa_pb.GetIndexResponse;
};

type PilosaDeleteIndex = {
  readonly methodName: string;
  readonly service: typeof Pilosa;
  readonly requestStream: false;
  readonly responseStream: false;
  readonly requestType: typeof pilosa_pb.DeleteIndexRequest;
  readonly responseType: typeof pilosa_pb.DeleteIndexResponse;
};

type PilosaQuerySQL = {
  readonly methodName: string;
  readonly service: typeof Pilosa;
  readonly requestStream: false;
  readonly responseStream: true;
  readonly requestType: typeof pilosa_pb.QuerySQLRequest;
  readonly responseType: typeof pilosa_pb.RowResponse;
};

type PilosaQuerySQLUnary = {
  readonly methodName: string;
  readonly service: typeof Pilosa;
  readonly requestStream: false;
  readonly responseStream: false;
  readonly requestType: typeof pilosa_pb.QuerySQLRequest;
  readonly responseType: typeof pilosa_pb.TableResponse;
};

type PilosaQueryPQL = {
  readonly methodName: string;
  readonly service: typeof Pilosa;
  readonly requestStream: false;
  readonly responseStream: true;
  readonly requestType: typeof pilosa_pb.QueryPQLRequest;
  readonly responseType: typeof pilosa_pb.RowResponse;
};

type PilosaQueryPQLUnary = {
  readonly methodName: string;
  readonly service: typeof Pilosa;
  readonly requestStream: false;
  readonly responseStream: false;
  readonly requestType: typeof pilosa_pb.QueryPQLRequest;
  readonly responseType: typeof pilosa_pb.TableResponse;
};

type PilosaInspect = {
  readonly methodName: string;
  readonly service: typeof Pilosa;
  readonly requestStream: false;
  readonly responseStream: true;
  readonly requestType: typeof pilosa_pb.InspectRequest;
  readonly responseType: typeof pilosa_pb.RowResponse;
};

export class Pilosa {
  static readonly serviceName: string;
  static readonly CreateIndex: PilosaCreateIndex;
  static readonly GetIndexes: PilosaGetIndexes;
  static readonly GetIndex: PilosaGetIndex;
  static readonly DeleteIndex: PilosaDeleteIndex;
  static readonly QuerySQL: PilosaQuerySQL;
  static readonly QuerySQLUnary: PilosaQuerySQLUnary;
  static readonly QueryPQL: PilosaQueryPQL;
  static readonly QueryPQLUnary: PilosaQueryPQLUnary;
  static readonly Inspect: PilosaInspect;
}

export type ServiceError = { message: string, code: number; metadata: grpc.Metadata }
export type Status = { details: string, code: number; metadata: grpc.Metadata }

interface UnaryResponse {
  cancel(): void;
}
interface ResponseStream<T> {
  cancel(): void;
  on(type: 'data', handler: (message: T) => void): ResponseStream<T>;
  on(type: 'end', handler: (status?: Status) => void): ResponseStream<T>;
  on(type: 'status', handler: (status: Status) => void): ResponseStream<T>;
}
interface RequestStream<T> {
  write(message: T): RequestStream<T>;
  end(): void;
  cancel(): void;
  on(type: 'end', handler: (status?: Status) => void): RequestStream<T>;
  on(type: 'status', handler: (status: Status) => void): RequestStream<T>;
}
interface BidirectionalStream<ReqT, ResT> {
  write(message: ReqT): BidirectionalStream<ReqT, ResT>;
  end(): void;
  cancel(): void;
  on(type: 'data', handler: (message: ResT) => void): BidirectionalStream<ReqT, ResT>;
  on(type: 'end', handler: (status?: Status) => void): BidirectionalStream<ReqT, ResT>;
  on(type: 'status', handler: (status: Status) => void): BidirectionalStream<ReqT, ResT>;
}

export class PilosaClient {
  readonly serviceHost: string;

  constructor(serviceHost: string, options?: grpc.RpcOptions);
  createIndex(
    requestMessage: pilosa_pb.CreateIndexRequest,
    metadata: grpc.Metadata,
    callback: (error: ServiceError|null, responseMessage: pilosa_pb.CreateIndexResponse|null) => void
  ): UnaryResponse;
  createIndex(
    requestMessage: pilosa_pb.CreateIndexRequest,
    callback: (error: ServiceError|null, responseMessage: pilosa_pb.CreateIndexResponse|null) => void
  ): UnaryResponse;
  getIndexes(
    requestMessage: pilosa_pb.GetIndexesRequest,
    metadata: grpc.Metadata,
    callback: (error: ServiceError|null, responseMessage: pilosa_pb.GetIndexesResponse|null) => void
  ): UnaryResponse;
  getIndexes(
    requestMessage: pilosa_pb.GetIndexesRequest,
    callback: (error: ServiceError|null, responseMessage: pilosa_pb.GetIndexesResponse|null) => void
  ): UnaryResponse;
  getIndex(
    requestMessage: pilosa_pb.GetIndexRequest,
    metadata: grpc.Metadata,
    callback: (error: ServiceError|null, responseMessage: pilosa_pb.GetIndexResponse|null) => void
  ): UnaryResponse;
  getIndex(
    requestMessage: pilosa_pb.GetIndexRequest,
    callback: (error: ServiceError|null, responseMessage: pilosa_pb.GetIndexResponse|null) => void
  ): UnaryResponse;
  deleteIndex(
    requestMessage: pilosa_pb.DeleteIndexRequest,
    metadata: grpc.Metadata,
    callback: (error: ServiceError|null, responseMessage: pilosa_pb.DeleteIndexResponse|null) => void
  ): UnaryResponse;
  deleteIndex(
    requestMessage: pilosa_pb.DeleteIndexRequest,
    callback: (error: ServiceError|null, responseMessage: pilosa_pb.DeleteIndexResponse|null) => void
  ): UnaryResponse;
  querySQL(requestMessage: pilosa_pb.QuerySQLRequest, metadata?: grpc.Metadata): ResponseStream<pilosa_pb.RowResponse>;
  querySQLUnary(
    requestMessage: pilosa_pb.QuerySQLRequest,
    metadata: grpc.Metadata,
    callback: (error: ServiceError|null, responseMessage: pilosa_pb.TableResponse|null) => void
  ): UnaryResponse;
  querySQLUnary(
    requestMessage: pilosa_pb.QuerySQLRequest,
    callback: (error: ServiceError|null, responseMessage: pilosa_pb.TableResponse|null) => void
  ): UnaryResponse;
  queryPQL(requestMessage: pilosa_pb.QueryPQLRequest, metadata?: grpc.Metadata): ResponseStream<pilosa_pb.RowResponse>;
  queryPQLUnary(
    requestMessage: pilosa_pb.QueryPQLRequest,
    metadata: grpc.Metadata,
    callback: (error: ServiceError|null, responseMessage: pilosa_pb.TableResponse|null) => void
  ): UnaryResponse;
  queryPQLUnary(
    requestMessage: pilosa_pb.QueryPQLRequest,
    callback: (error: ServiceError|null, responseMessage: pilosa_pb.TableResponse|null) => void
  ): UnaryResponse;
  inspect(requestMessage: pilosa_pb.InspectRequest, metadata?: grpc.Metadata): ResponseStream<pilosa_pb.RowResponse>;
}

