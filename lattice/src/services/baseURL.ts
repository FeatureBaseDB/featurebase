import { pilosaConfig } from 'lattice.config';

const hostname = pilosaConfig.hostname
  ? pilosaConfig.hostname
  : window.location.hostname;
const port = pilosaConfig.httpPort
  ? pilosaConfig.httpPort
  : window.location.port;

export const baseURL = `${window.location.protocol}//${hostname}:${port}`;
