import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { formatTime } from '@/lib/formatTime';
import { cn } from '@/lib/utils';
import type { QueryRecord } from '@/orval/models';

interface QueriesHistoryTableProps {
  isLoading: boolean;
  queries: QueryRecord[];
}

export function QueriesHistoryTable({ isLoading, queries }: QueriesHistoryTableProps) {
  const columnHelper = createColumnHelper<QueryRecord>();

  const columns = [
    columnHelper.accessor('id', {
      header: 'ID',
    }),
    columnHelper.accessor('query', {
      header: 'SQL',
      meta: {
        cellClassName: 'max-w-[300px] truncate',
      },
    }),
    columnHelper.accessor('status', {
      header: 'Status',
      cell: (info) => {
        const status = info.getValue();
        return (
          <Badge variant="outline">
            <span
              className={cn(
                'capitalize',
                status === 'successful' && 'text-green-500',
                status === 'failed' && 'text-red-500',
              )}
            >
              {status}
            </span>
          </Badge>
        );
      },
    }),
    columnHelper.accessor('startTime', {
      header: 'Start Time',
      cell: (info) => {
        return <span>{formatTime(info.getValue())}</span>;
      },
    }),
    columnHelper.accessor('durationMs', {
      header: 'Duration',
      cell: (info) => {
        const maxDuration = Math.max(...queries.map((query) => query.durationMs));
        const percentageFromMaxDuration = (info.getValue() / maxDuration) * 100;
        return (
          <div className="flex min-w-[100px] items-center gap-2">
            <Progress value={percentageFromMaxDuration} />
            <span>{info.getValue()}ms</span>
          </div>
        );
      },
    }),
  ];

  return <DataTable rounded columns={columns} data={queries} isLoading={isLoading} />;
}
