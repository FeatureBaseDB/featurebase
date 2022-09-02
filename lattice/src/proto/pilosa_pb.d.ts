// package: pilosa
// file: pilosa.proto

import * as jspb from "google-protobuf";

export class QueryPQLRequest extends jspb.Message {
  getIndex(): string;
  setIndex(value: string): void;

  getPql(): string;
  setPql(value: string): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): QueryPQLRequest.AsObject;
  static toObject(includeInstance: boolean, msg: QueryPQLRequest): QueryPQLRequest.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: QueryPQLRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): QueryPQLRequest;
  static deserializeBinaryFromReader(message: QueryPQLRequest, reader: jspb.BinaryReader): QueryPQLRequest;
}

export namespace QueryPQLRequest {
  export type AsObject = {
    index: string,
    pql: string,
  }
}

export class QuerySQLRequest extends jspb.Message {
  getSql(): string;
  setSql(value: string): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): QuerySQLRequest.AsObject;
  static toObject(includeInstance: boolean, msg: QuerySQLRequest): QuerySQLRequest.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: QuerySQLRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): QuerySQLRequest;
  static deserializeBinaryFromReader(message: QuerySQLRequest, reader: jspb.BinaryReader): QuerySQLRequest;
}

export namespace QuerySQLRequest {
  export type AsObject = {
    sql: string,
  }
}

export class StatusError extends jspb.Message {
  getCode(): number;
  setCode(value: number): void;

  getMessage(): string;
  setMessage(value: string): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): StatusError.AsObject;
  static toObject(includeInstance: boolean, msg: StatusError): StatusError.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: StatusError, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): StatusError;
  static deserializeBinaryFromReader(message: StatusError, reader: jspb.BinaryReader): StatusError;
}

export namespace StatusError {
  export type AsObject = {
    code: number,
    message: string,
  }
}

export class RowResponse extends jspb.Message {
  clearHeadersList(): void;
  getHeadersList(): Array<ColumnInfo>;
  setHeadersList(value: Array<ColumnInfo>): void;
  addHeaders(value?: ColumnInfo, index?: number): ColumnInfo;

  clearColumnsList(): void;
  getColumnsList(): Array<ColumnResponse>;
  setColumnsList(value: Array<ColumnResponse>): void;
  addColumns(value?: ColumnResponse, index?: number): ColumnResponse;

  hasStatuserror(): boolean;
  clearStatuserror(): void;
  getStatuserror(): StatusError | undefined;
  setStatuserror(value?: StatusError): void;

  getDuration(): number;
  setDuration(value: number): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): RowResponse.AsObject;
  static toObject(includeInstance: boolean, msg: RowResponse): RowResponse.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: RowResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): RowResponse;
  static deserializeBinaryFromReader(message: RowResponse, reader: jspb.BinaryReader): RowResponse;
}

export namespace RowResponse {
  export type AsObject = {
    headersList: Array<ColumnInfo.AsObject>,
    columnsList: Array<ColumnResponse.AsObject>,
    statuserror?: StatusError.AsObject,
    duration: number,
  }
}

export class Row extends jspb.Message {
  clearColumnsList(): void;
  getColumnsList(): Array<ColumnResponse>;
  setColumnsList(value: Array<ColumnResponse>): void;
  addColumns(value?: ColumnResponse, index?: number): ColumnResponse;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Row.AsObject;
  static toObject(includeInstance: boolean, msg: Row): Row.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: Row, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Row;
  static deserializeBinaryFromReader(message: Row, reader: jspb.BinaryReader): Row;
}

export namespace Row {
  export type AsObject = {
    columnsList: Array<ColumnResponse.AsObject>,
  }
}

export class TableResponse extends jspb.Message {
  clearHeadersList(): void;
  getHeadersList(): Array<ColumnInfo>;
  setHeadersList(value: Array<ColumnInfo>): void;
  addHeaders(value?: ColumnInfo, index?: number): ColumnInfo;

  clearRowsList(): void;
  getRowsList(): Array<Row>;
  setRowsList(value: Array<Row>): void;
  addRows(value?: Row, index?: number): Row;

  hasStatuserror(): boolean;
  clearStatuserror(): void;
  getStatuserror(): StatusError | undefined;
  setStatuserror(value?: StatusError): void;

  getDuration(): number;
  setDuration(value: number): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): TableResponse.AsObject;
  static toObject(includeInstance: boolean, msg: TableResponse): TableResponse.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: TableResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): TableResponse;
  static deserializeBinaryFromReader(message: TableResponse, reader: jspb.BinaryReader): TableResponse;
}

export namespace TableResponse {
  export type AsObject = {
    headersList: Array<ColumnInfo.AsObject>,
    rowsList: Array<Row.AsObject>,
    statuserror?: StatusError.AsObject,
    duration: number,
  }
}

export class ColumnInfo extends jspb.Message {
  getName(): string;
  setName(value: string): void;

  getDatatype(): string;
  setDatatype(value: string): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): ColumnInfo.AsObject;
  static toObject(includeInstance: boolean, msg: ColumnInfo): ColumnInfo.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: ColumnInfo, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): ColumnInfo;
  static deserializeBinaryFromReader(message: ColumnInfo, reader: jspb.BinaryReader): ColumnInfo;
}

export namespace ColumnInfo {
  export type AsObject = {
    name: string,
    datatype: string,
  }
}

export class ColumnResponse extends jspb.Message {
  hasStringval(): boolean;
  clearStringval(): void;
  getStringval(): string;
  setStringval(value: string): void;

  hasUint64val(): boolean;
  clearUint64val(): void;
  getUint64val(): number;
  setUint64val(value: number): void;

  hasInt64val(): boolean;
  clearInt64val(): void;
  getInt64val(): number;
  setInt64val(value: number): void;

  hasBoolval(): boolean;
  clearBoolval(): void;
  getBoolval(): boolean;
  setBoolval(value: boolean): void;

  hasBlobval(): boolean;
  clearBlobval(): void;
  getBlobval(): Uint8Array | string;
  getBlobval_asU8(): Uint8Array;
  getBlobval_asB64(): string;
  setBlobval(value: Uint8Array | string): void;

  hasUint64arrayval(): boolean;
  clearUint64arrayval(): void;
  getUint64arrayval(): Uint64Array | undefined;
  setUint64arrayval(value?: Uint64Array): void;

  hasStringarrayval(): boolean;
  clearStringarrayval(): void;
  getStringarrayval(): StringArray | undefined;
  setStringarrayval(value?: StringArray): void;

  hasFloat64val(): boolean;
  clearFloat64val(): void;
  getFloat64val(): number;
  setFloat64val(value: number): void;

  hasDecimalval(): boolean;
  clearDecimalval(): void;
  getDecimalval(): Decimal | undefined;
  setDecimalval(value?: Decimal): void;

  hasTimestampval(): boolean;
  clearTimestampval(): void;
  getTimestampval(): string;
  setTimestampval(value: string): void;

  getColumnvalCase(): ColumnResponse.ColumnvalCase;
  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): ColumnResponse.AsObject;
  static toObject(includeInstance: boolean, msg: ColumnResponse): ColumnResponse.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: ColumnResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): ColumnResponse;
  static deserializeBinaryFromReader(message: ColumnResponse, reader: jspb.BinaryReader): ColumnResponse;
}

export namespace ColumnResponse {
  export type AsObject = {
    stringval: string,
    uint64val: number,
    int64val: number,
    boolval: boolean,
    blobval: Uint8Array | string,
    uint64arrayval?: Uint64Array.AsObject,
    stringarrayval?: StringArray.AsObject,
    float64val: number,
    decimalval?: Decimal.AsObject,
    timestampval: string,
  }

  export enum ColumnvalCase {
    COLUMNVAL_NOT_SET = 0,
    STRINGVAL = 1,
    UINT64VAL = 2,
    INT64VAL = 3,
    BOOLVAL = 4,
    BLOBVAL = 5,
    UINT64ARRAYVAL = 6,
    STRINGARRAYVAL = 7,
    FLOAT64VAL = 8,
    DECIMALVAL = 9,
    TIMESTAMPVAL = 10,
  }
}

export class Decimal extends jspb.Message {
  getValue(): number;
  setValue(value: number): void;

  getScale(): number;
  setScale(value: number): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Decimal.AsObject;
  static toObject(includeInstance: boolean, msg: Decimal): Decimal.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: Decimal, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Decimal;
  static deserializeBinaryFromReader(message: Decimal, reader: jspb.BinaryReader): Decimal;
}

export namespace Decimal {
  export type AsObject = {
    value: number,
    scale: number,
  }
}

export class InspectRequest extends jspb.Message {
  getIndex(): string;
  setIndex(value: string): void;

  hasColumns(): boolean;
  clearColumns(): void;
  getColumns(): IdsOrKeys | undefined;
  setColumns(value?: IdsOrKeys): void;

  clearFilterfieldsList(): void;
  getFilterfieldsList(): Array<string>;
  setFilterfieldsList(value: Array<string>): void;
  addFilterfields(value: string, index?: number): string;

  getLimit(): number;
  setLimit(value: number): void;

  getOffset(): number;
  setOffset(value: number): void;

  getQuery(): string;
  setQuery(value: string): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): InspectRequest.AsObject;
  static toObject(includeInstance: boolean, msg: InspectRequest): InspectRequest.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: InspectRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): InspectRequest;
  static deserializeBinaryFromReader(message: InspectRequest, reader: jspb.BinaryReader): InspectRequest;
}

export namespace InspectRequest {
  export type AsObject = {
    index: string,
    columns?: IdsOrKeys.AsObject,
    filterfieldsList: Array<string>,
    limit: number,
    offset: number,
    query: string,
  }
}

export class Uint64Array extends jspb.Message {
  clearValsList(): void;
  getValsList(): Array<number>;
  setValsList(value: Array<number>): void;
  addVals(value: number, index?: number): number;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Uint64Array.AsObject;
  static toObject(includeInstance: boolean, msg: Uint64Array): Uint64Array.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: Uint64Array, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Uint64Array;
  static deserializeBinaryFromReader(message: Uint64Array, reader: jspb.BinaryReader): Uint64Array;
}

export namespace Uint64Array {
  export type AsObject = {
    valsList: Array<number>,
  }
}

export class StringArray extends jspb.Message {
  clearValsList(): void;
  getValsList(): Array<string>;
  setValsList(value: Array<string>): void;
  addVals(value: string, index?: number): string;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): StringArray.AsObject;
  static toObject(includeInstance: boolean, msg: StringArray): StringArray.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: StringArray, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): StringArray;
  static deserializeBinaryFromReader(message: StringArray, reader: jspb.BinaryReader): StringArray;
}

export namespace StringArray {
  export type AsObject = {
    valsList: Array<string>,
  }
}

export class IdsOrKeys extends jspb.Message {
  hasIds(): boolean;
  clearIds(): void;
  getIds(): Uint64Array | undefined;
  setIds(value?: Uint64Array): void;

  hasKeys(): boolean;
  clearKeys(): void;
  getKeys(): StringArray | undefined;
  setKeys(value?: StringArray): void;

  getTypeCase(): IdsOrKeys.TypeCase;
  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): IdsOrKeys.AsObject;
  static toObject(includeInstance: boolean, msg: IdsOrKeys): IdsOrKeys.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: IdsOrKeys, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): IdsOrKeys;
  static deserializeBinaryFromReader(message: IdsOrKeys, reader: jspb.BinaryReader): IdsOrKeys;
}

export namespace IdsOrKeys {
  export type AsObject = {
    ids?: Uint64Array.AsObject,
    keys?: StringArray.AsObject,
  }

  export enum TypeCase {
    TYPE_NOT_SET = 0,
    IDS = 1,
    KEYS = 2,
  }
}

export class Index extends jspb.Message {
  getName(): string;
  setName(value: string): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Index.AsObject;
  static toObject(includeInstance: boolean, msg: Index): Index.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: Index, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Index;
  static deserializeBinaryFromReader(message: Index, reader: jspb.BinaryReader): Index;
}

export namespace Index {
  export type AsObject = {
    name: string,
  }
}

export class CreateIndexRequest extends jspb.Message {
  getName(): string;
  setName(value: string): void;

  getKeys(): boolean;
  setKeys(value: boolean): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): CreateIndexRequest.AsObject;
  static toObject(includeInstance: boolean, msg: CreateIndexRequest): CreateIndexRequest.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: CreateIndexRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): CreateIndexRequest;
  static deserializeBinaryFromReader(message: CreateIndexRequest, reader: jspb.BinaryReader): CreateIndexRequest;
}

export namespace CreateIndexRequest {
  export type AsObject = {
    name: string,
    keys: boolean,
  }
}

export class CreateIndexResponse extends jspb.Message {
  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): CreateIndexResponse.AsObject;
  static toObject(includeInstance: boolean, msg: CreateIndexResponse): CreateIndexResponse.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: CreateIndexResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): CreateIndexResponse;
  static deserializeBinaryFromReader(message: CreateIndexResponse, reader: jspb.BinaryReader): CreateIndexResponse;
}

export namespace CreateIndexResponse {
  export type AsObject = {
  }
}

export class GetIndexRequest extends jspb.Message {
  getName(): string;
  setName(value: string): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetIndexRequest.AsObject;
  static toObject(includeInstance: boolean, msg: GetIndexRequest): GetIndexRequest.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: GetIndexRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetIndexRequest;
  static deserializeBinaryFromReader(message: GetIndexRequest, reader: jspb.BinaryReader): GetIndexRequest;
}

export namespace GetIndexRequest {
  export type AsObject = {
    name: string,
  }
}

export class GetIndexResponse extends jspb.Message {
  hasIndex(): boolean;
  clearIndex(): void;
  getIndex(): Index | undefined;
  setIndex(value?: Index): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetIndexResponse.AsObject;
  static toObject(includeInstance: boolean, msg: GetIndexResponse): GetIndexResponse.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: GetIndexResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetIndexResponse;
  static deserializeBinaryFromReader(message: GetIndexResponse, reader: jspb.BinaryReader): GetIndexResponse;
}

export namespace GetIndexResponse {
  export type AsObject = {
    index?: Index.AsObject,
  }
}

export class GetIndexesRequest extends jspb.Message {
  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetIndexesRequest.AsObject;
  static toObject(includeInstance: boolean, msg: GetIndexesRequest): GetIndexesRequest.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: GetIndexesRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetIndexesRequest;
  static deserializeBinaryFromReader(message: GetIndexesRequest, reader: jspb.BinaryReader): GetIndexesRequest;
}

export namespace GetIndexesRequest {
  export type AsObject = {
  }
}

export class GetIndexesResponse extends jspb.Message {
  clearIndexesList(): void;
  getIndexesList(): Array<Index>;
  setIndexesList(value: Array<Index>): void;
  addIndexes(value?: Index, index?: number): Index;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetIndexesResponse.AsObject;
  static toObject(includeInstance: boolean, msg: GetIndexesResponse): GetIndexesResponse.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: GetIndexesResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetIndexesResponse;
  static deserializeBinaryFromReader(message: GetIndexesResponse, reader: jspb.BinaryReader): GetIndexesResponse;
}

export namespace GetIndexesResponse {
  export type AsObject = {
    indexesList: Array<Index.AsObject>,
  }
}

export class DeleteIndexRequest extends jspb.Message {
  getName(): string;
  setName(value: string): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): DeleteIndexRequest.AsObject;
  static toObject(includeInstance: boolean, msg: DeleteIndexRequest): DeleteIndexRequest.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: DeleteIndexRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): DeleteIndexRequest;
  static deserializeBinaryFromReader(message: DeleteIndexRequest, reader: jspb.BinaryReader): DeleteIndexRequest;
}

export namespace DeleteIndexRequest {
  export type AsObject = {
    name: string,
  }
}

export class DeleteIndexResponse extends jspb.Message {
  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): DeleteIndexResponse.AsObject;
  static toObject(includeInstance: boolean, msg: DeleteIndexResponse): DeleteIndexResponse.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: DeleteIndexResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): DeleteIndexResponse;
  static deserializeBinaryFromReader(message: DeleteIndexResponse, reader: jspb.BinaryReader): DeleteIndexResponse;
}

export namespace DeleteIndexResponse {
  export type AsObject = {
  }
}

