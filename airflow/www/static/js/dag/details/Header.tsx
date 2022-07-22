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

import React, { useEffect } from 'react';
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  Text,
} from '@chakra-ui/react';

import { getMetaValue } from 'src/utils';
import useSelection from 'src/dag/useSelection';
import Time from 'src/components/Time';
import { useTasks, useGridData } from 'src/api';
import RunTypeIcon from 'src/components/RunTypeIcon';

import BreadcrumbText from './BreadcrumbText';

const dagId = getMetaValue('dag_id');

const Header = () => {
  const { data: { dagRuns } } = useGridData();
  const { data: { tasks } } = useTasks();

  const { selected: { taskId, runId }, onSelect, clearSelection } = useSelection();
  const dagRun = dagRuns.find((r) => r.runId === runId);
  const task = tasks.find((t) => t.taskId === taskId);

  // clearSelection if the current selected dagRun is
  // filtered out.
  useEffect(() => {
    if (runId && !dagRun) {
      clearSelection();
    }
  }, [clearSelection, dagRun, runId]);

  let runLabel;
  if (dagRun && runId) {
    // If a runId includes the runtype then parse the time, otherwise use the custom run id
    const runName = (
      runId.includes('manual__')
      || runId.includes('scheduled__')
      || runId.includes('backfill__')
      || runId.includes('dataset_triggered__')
    )
      ? <Time dateTime={dagRun.dataIntervalStart || dagRun.executionDate} />
      : runId;
    runLabel = (
      <>
        <RunTypeIcon runType={dagRun.runType} />
        {runName}
      </>
    );
  }

  const isMapped = task && task.isMapped;
  const lastIndex = taskId ? taskId.lastIndexOf('.') : null;
  const taskName = taskId && lastIndex ? taskId.substring(lastIndex + 1) : taskId;

  const isDagDetails = !runId && !taskId;
  const isRunDetails = !!(runId && !taskId);
  const isTaskDetails = runId && taskId;

  return (
    <Breadcrumb separator={<Text color="gray.300">/</Text>}>
      <BreadcrumbItem isCurrentPage={isDagDetails} mt={4}>
        <BreadcrumbLink onClick={clearSelection} _hover={isDagDetails ? { cursor: 'default' } : undefined}>
          <BreadcrumbText label="DAG" value={dagId} />
        </BreadcrumbLink>
      </BreadcrumbItem>
      {runId && (
        <BreadcrumbItem isCurrentPage={isRunDetails} mt={4}>
          <BreadcrumbLink onClick={() => onSelect({ runId })} _hover={isRunDetails ? { cursor: 'default' } : undefined}>
            <BreadcrumbText label="Run" value={runLabel} />
          </BreadcrumbLink>
        </BreadcrumbItem>
      )}
      {taskId && (
        <BreadcrumbItem isCurrentPage mt={4}>
          <BreadcrumbLink _hover={isTaskDetails ? { cursor: 'default' } : undefined}>
            <BreadcrumbText label="Task" value={`${taskName}${isMapped ? ' []' : ''}`} />
          </BreadcrumbLink>
        </BreadcrumbItem>
      )}
    </Breadcrumb>
  );
};

export default Header;
