/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, {
  useState, useMemo, useEffect,
} from 'react';
import {
  Flex,
  Text,
  Box,
} from '@chakra-ui/react';
import { snakeCase } from 'lodash';
import type { SortingRule } from 'react-table';

import { formatDuration, getDuration } from 'src/datetime_utils';
import { useMappedInstances } from 'src/api';
import { SimpleStatus } from 'src/dag/StatusBox';
import { Table } from 'src/components/Table';
import Time from 'src/components/Time';
import type { API, TaskInstance } from 'src/types';

interface Props {
  dagId: string;
  runId: string;
  taskId: string;
  onRowClicked: (rowMapIndex: number, taskInstances: TaskInstance[]) => void;
  mapIndex?: TaskInstance['mapIndex'];
  onMappedInstancesFetch: (mappedTaskInstances: TaskInstance[]) => void;
}

/* GridData.TaskInstance and API.TaskInstance are not compatible at the moment.
 * Remove this function when changing the api response for grid_data_url to comply
 * with API.TaskInstance.
 */
const convertTaskInstances = (taskInstances: API.TaskInstance[]) => taskInstances.map(
  (ti) => ({ ...ti, runId: ti.dagRunId }) as TaskInstance,
);

const MappedInstances = ({
  dagId, runId, taskId, onRowClicked, onMappedInstancesFetch, mapIndex,
}: Props) => {
  const limit = 25;
  const [offset, setOffset] = useState(0);
  const [sortBy, setSortBy] = useState<SortingRule<object>[]>([]);

  const sort = sortBy[0];

  const order = sort && (sort.id === 'state' || sort.id === 'mapIndex') ? `${sort.desc ? '-' : ''}${snakeCase(sort.id)}` : '';

  const {
    data: { taskInstances, totalEntries } = { taskInstances: [], totalEntries: 0 },
    isLoading,
  } = useMappedInstances({
    dagId, runId, taskId, limit, offset, order,
  });

  const convertedTaskInstances = useMemo(
    () => convertTaskInstances(taskInstances),
    [taskInstances],
  );

  useEffect(() => {
    onMappedInstancesFetch(convertedTaskInstances);
  }, [mapIndex, onMappedInstancesFetch, convertedTaskInstances]);

  const data = useMemo(() => taskInstances.map((mi) => ({
    ...mi,
    state: (
      <Flex alignItems="center">
        <SimpleStatus state={mi.state === undefined || mi.state === 'none' ? null : mi.state} mx={2} />
        {mi.state || 'no status'}
      </Flex>
    ),
    duration: mi.duration && formatDuration(getDuration(mi.startDate, mi.endDate)),
    startDate: <Time dateTime={mi.startDate} />,
    endDate: <Time dateTime={mi.endDate} />,
  })), [taskInstances]);

  const columns = useMemo(
    () => [
      {
        Header: 'Map Index',
        accessor: 'mapIndex',
      },
      {
        Header: 'State',
        accessor: 'state',
      },
      {
        Header: 'Duration',
        accessor: 'duration',
        disableSortBy: true,
      },
      {
        Header: 'Start Date',
        accessor: 'startDate',
        disableSortBy: true,
      },
      {
        Header: 'End Date',
        accessor: 'endDate',
        disableSortBy: true,
      },
    ],
    [],
  );

  if (mapIndex !== undefined) { return null; }

  return (
    <Box>
      <br />
      <Text as="strong">Mapped Instances</Text>
      <Table
        data={data}
        columns={columns}
        manualPagination={{
          offset,
          setOffset,
          totalEntries,
        }}
        pageSize={limit}
        manualSort={{
          setSortBy,
          sortBy,
        }}
        isLoading={isLoading}
        onRowClicked={
          (row) => onRowClicked(row.values.mapIndex, convertedTaskInstances)
        }
      />
    </Box>
  );
};

export default MappedInstances;
