import axios from 'axios';

import { baseURL } from './baseURL';

const api = axios.create({
  baseURL,
  headers: {
    'Content-Type': 'application/x-www-form-urlencoded',
    Accept: 'application/json',
  },
});

export const pilosa = {
  get: {
    status() {
      return api.get('/status');
    },
    auth() {
      return api.get('/auth');
    },
    userinfo() {
      return api.get('/userinfo');
    },
    info() {
      return api.get('/info');
    },
    version() {
      return api.get('/version');
    },
    transactions() {
      return api.get('/ui/transaction');
    },
    transaction(id) {
      return api.get(`/transaction/${id}`);
    },
    schema() {
      return api.get('/schema');
    },
    schemaDetails() {
      return api.get('/schema/details');
    },
    metrics() {
      return api.get('/metrics.json');
    },
    queryHistory() {
      return api.get('/query-history');
    }
  },
  post: {
    finishTransaction(id) {
      return api.post(`/transaction/${id}/finish`);
    },
    query(index, query) {
      return api.post(`/index/${index}/query`, query);
    },
    sql(query) {
      return api.post(`/sql`, query);
    }
  },
};
