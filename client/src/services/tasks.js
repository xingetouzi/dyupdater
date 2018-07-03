import { stringify } from 'qs';
import request from '../utils/request';
import config from '../config';

const { apiPrefix } = config;

export async function query(params) {
  return request(`${apiPrefix}/tasks?${stringify(params)}`);
}
