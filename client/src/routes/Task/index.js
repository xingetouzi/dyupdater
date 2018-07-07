import React, { PureComponent } from 'react';
import { connect } from 'dva';
import { Card, Badge, Switch, Button } from 'antd';
import StandardTable from 'components/StandardTable';

import styles from './index.less';
import { taskType, taskState } from '../../utils/enum';

const getValue = obj =>
  Object.keys(obj)
    .map(key => obj[key])
    .join(',');

const taskTypeText = {
  [taskType.CHECK]: '检查',
  [taskType.CAL]: '计算',
  [taskType.UPDATE]: '更新',
};
const taskTypeColor = {
  [taskType.CHECK]: '#0033cc',
  [taskType.CAL]: '#ff6600',
  [taskType.UPDATE]: '#00cc00',
};
const statusMap = {
  [taskState.PENDING]: 'processing',
  [taskState.SUCCESS]: 'success',
  [taskState.FAILED]: 'error',
};
const statusText = {
  [taskState.PENDING]: '进行中',
  [taskState.SUCCESS]: '成功',
  [taskState.FAILED]: '失败',
};

const pageSize = 10;

@connect(({ task, loading }) => ({
  task,
  loading: loading.models.task,
}))
export default class TaskTableList extends PureComponent {
  state = {
    selectedRows: [],
    treeview: false,
    pagination: {
      current: 1,
      pageSize,
    },
    formValues: {},
    filters: {},
    sorter: {},
  };

  componentDidMount() {
    this.fetch();
  }

  fetch() {
    const { pagination, treeview, formValues, filters, sorter } = this.state;

    const params = {
      currentPage: pagination.current,
      pageSize: pagination.pageSize,
      recursive: treeview,
      ...formValues,
      ...filters,
    };

    if (sorter.field) {
      params.sorter = `${sorter.field}_${sorter.order}`;
    }

    this.props.dispatch({
      type: 'task/fetch',
      payload: params,
    });
  }

  handleSelectRows = rows => {
    this.setState({
      selectedRows: rows,
    });
  };

  handleStandardTableChange = (pagination, filtersArg, sorter) => {
    const { dispatch } = this.props;
    const { formValues, treeview } = this.state;

    const filters = Object.keys(filtersArg).reduce((obj, key) => {
      const newObj = { ...obj };
      newObj[key] = getValue(filtersArg[key]);
      return newObj;
    }, {});

    this.setState({ pagination, filters, sorter });
    const params = {
      currentPage: pagination.current,
      pageSize: pagination.pageSize,
      recursive: treeview,
      ...formValues,
      ...filters,
    };
    if (sorter.field) {
      params.sorter = `${sorter.field}_${sorter.order}`;
    }

    dispatch({
      type: 'task/fetch',
      payload: params,
    });
  };

  handleSwitchChange = checked => {
    this.setState({ treeview: checked }, () => {
      this.fetch();
    });
  };

  render() {
    const { task: { data }, loading } = this.props;
    const { selectedRows, treeview } = this.state;

    const columns = [
      {
        title: '任务ID',
        dataIndex: 'id',
        sorter: true,
        width: 200,
      },
      {
        title: '父任务ID',
        dataIndex: 'parent',
        width: 110,
      },
      {
        title: '类型',
        dataIndex: 'type',
        filters: [
          {
            text: taskTypeText[0],
            value: 0,
          },
          {
            text: taskTypeText[1],
            value: 1,
          },
          {
            text: taskTypeText[2],
            value: 2,
          },
        ],
        render: item => (
          <div style={{ color: taskTypeColor[item] }}>{taskTypeText[item] || 'UNKNOWN'}</div>
        ),
        width: 90,
      },
      {
        title: '因子',
        dataIndex: 'factor',
        sorter: true,
        width: 150,
      },
      {
        title: '时间范围',
        render: (item, record) => (
          <div>
            <span>{record.start}</span>
            <br />
            <span>{record.end}</span>
          </div>
        ),
        width: 100,
      },
      {
        title: '发布时间',
        dataIndex: 'published',
        sorter: true,
        width: 170,
      },
      {
        title: '更新时间',
        dataIndex: 'updated',
        sorter: true,
        width: 170,
      },
      {
        title: '状态',
        dataIndex: 'status',
        filters: [
          {
            text: statusText[0],
            value: 0,
          },
          {
            text: statusText[1],
            value: 1,
          },
          {
            text: statusText[2],
            value: 2,
          },
        ],
        render(item, record) {
          return (
            <div>
              <Badge status={statusMap[item] || 'default'} text={statusText[item] || 'UNKNOWN'} />
              {record.error && <span style={{ color: 'red' }}>X{record.retry}</span>}
            </div>
          );
        },
        width: 110,
      },
      {
        title: '错误信息',
        dataIndex: 'error',
        width: 400,
      },
    ];

    return (
      <div className={styles.standardList}>
        <Card bordered={false} title="工作任务列表" style={{ marginTop: 16 }}>
          <div>
            树形视图:{' '}
            <Switch
              checkedChildren="开"
              unCheckedChildren="关"
              checked={treeview}
              onChange={this.handleSwitchChange}
            />
            <Button
              icon="reload"
              style={{ float: 'right' }}
              type="primary"
              onClick={() => {
                this.fetch();
              }}
            />
          </div>
          <StandardTable
            rowKey="id"
            selectedRows={selectedRows}
            loading={loading}
            data={data}
            columns={columns}
            onSelectRow={this.handleSelectRows}
            onChange={this.handleStandardTableChange}
          />
        </Card>
      </div>
    );
  }
}
