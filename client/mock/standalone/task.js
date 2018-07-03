import mockjs from 'mockjs';
import { apiPrefix } from '../../src/config';

// 引入分离的 mock 文件

const taskType = {
  CHECK: 0,
  CAL: 1,
  UPDATE: 2,
};

const taskState = {
  PENDING: 0,
  SUCCESS: 1,
  FAILED: 2,
};

const idOffset = 100000000;

let id = 0;

const nextTaskID = () => {
  id += 1;
  return (id + idOffset).toString();
};

const getTaskParent = () => {
  if (id === 1) {
    return '';
  }
  if (id <= 3) {
    return (id - 1 + idOffset).toString();
  }
  if (Math.random() > 0.8) {
    return '';
  }
  return (Math.ceil(Math.random() * 3) + idOffset).toString();
};

const taskListData = mockjs.mock({
  'data|80-100': [
    {
      id: nextTaskID,
      parent: getTaskParent,
      'type|1': Object.values(taskType),
      'status|1': Object.values(taskState),
      'retry|1': [0, 1, 2, 3],
      factor: '@word(7)',
      start: '@date(yyyyMMdd)',
      end: '@date(yyyyMMdd)',
      published: '@date(yy/MM/dd HH:mm:ss)',
      updated: '@date(yy/MM/dd HH:mm:ss)',
      error: '@sentence',
    },
  ],
});

const getChildren = data => {
  const childrenMap = {};
  for (const item of data) {
    if (item.parent !== '') {
      let children = childrenMap[item.parent];
      if (children === undefined) {
        childrenMap[item.parent] = [];
        children = childrenMap[item.parent];
      }
      children.push(item);
    }
  }
  for (const item of data) {
    const children = childrenMap[item.id];
    if (children && children.length) {
      item.children = children;
    }
  }
  return data;
};

const taskDataSouce = getChildren(taskListData.data);

function checkValue(val, template) {
  const temps = decodeURI(template)
    .trim()
    .split(',');
  return temps.some(
    temp =>
      String(val)
        .trim()
        .indexOf(temp) > -1
  );
}

module.exports = {
  // tradings
  [`GET ${apiPrefix}/tasks`](req, res) {
    const { query } = req;
    let { pageSize, currentPage, recursive, ...other } = query; // eslint-disable-line
    pageSize = parseInt(pageSize, 10) || 10;
    currentPage = parseInt(currentPage, 10) || 1;
    let newData = taskDataSouce;
    for (const key in other) {
      if ({}.hasOwnProperty.call(other, key)) {
        newData = newData.filter(item => {
          if ({}.hasOwnProperty.call(item, key)) {
            return checkValue(item[key], other[key]);
          }
          return true;
        });
      }
    }
    if (recursive && recursive !== 'false') {
      newData = newData.filter(item => item.parent === '');
    } else {
      newData = newData.map(item => {
        const { children, ...newItem } = item;
        return newItem;
      });
    }
    res.status(200).json({
      list: newData.slice((currentPage - 1) * pageSize, currentPage * pageSize),
      pagination: {
        total: newData.length,
        pageSize,
        currentPage,
      },
    });
  },
};
