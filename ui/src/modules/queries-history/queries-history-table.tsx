import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import type { QueryRecord } from '@/orval/models';

interface QueriesHistoryTableProps {
  isLoading: boolean;
  queries: QueryRecord[];
}

export function QueriesHistoryTable({ isLoading, queries }: QueriesHistoryTableProps) {
  const columnHelper = createColumnHelper<QueryRecord>();

  const columns = [
    columnHelper.accessor('id', {
      header: 'Id',
    }),
    columnHelper.accessor('query', {
      header: 'Query',
      meta: {
        cellClassName: 'max-w-[300px] truncate',
      },
    }),
    columnHelper.accessor('status', {
      header: 'Status',
    }),
    columnHelper.accessor('startTime', {
      header: 'Start Time',
      cell: (info) => {
        const date = new Date(info.getValue()).toLocaleTimeString('en-US', {
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
        });
        return <span>{date}</span>;
      },
    }),
    columnHelper.accessor('durationMs', {
      header: 'Duration',
    }),
  ];

  return <DataTable rounded columns={columns} data={queries} isLoading={isLoading} />;
}
