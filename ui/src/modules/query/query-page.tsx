import { Link, useParams } from '@tanstack/react-router';
import { ArrowLeftIcon, DatabaseZap } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { Skeleton } from '@/components/ui/skeleton';
import { useGetQuery, useGetQueryResult } from '@/orval/queries';

import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { QueryDetails } from './query-details';
import { QueryResultsTable } from './query-result-table';
import { QuerySQL } from './query-sql';

export function QueryPage() {
  const { queryId } = useParams({ from: '/queries/$queryId/' });

  const { data: queryRecord, isLoading: queryRecordLoading } = useGetQuery(+queryId);

  const { data: queryRecordResult, isFetching: queryRecordResultLoading } = useGetQueryResult(
    +queryId,
    {
      query: {
        enabled: queryRecord?.status === 'successful',
      },
    },
  );
  const columns = queryRecordResult?.columns ?? [];
  const rows = queryRecordResult?.rows ?? [];
  const loading = queryRecordLoading || queryRecordResultLoading;

  // const { detailsRef, tableStyle } = useMeasureQueryResultsHeight({ isReady: !isLoading });

  return (
    <>
      <PageHeader
        title={
          <div className="flex items-center gap-2">
            <Link to="/queries">
              <Button variant="outline" size="icon" className="size-8">
                <ArrowLeftIcon className="size-4" />
              </Button>
            </Link>
            {queryRecord ? (
              <h1 className="text-lg">Query - {queryRecord.id}</h1>
            ) : (
              <Skeleton className="h-7 w-[204px]" />
            )}
          </div>
        }
      />
      {!queryRecord && !loading ? (
        <PageEmptyContainer
          Icon={DatabaseZap}
          title="Query not found"
          description="The query you are looking for does not exist."
        />
      ) : (
        <>
          <div className="p-4">
            <QueryDetails queryRecord={queryRecord} />
          </div>

          <ScrollArea tableViewport className="mx-4 h-[calc(100%-244px)]">
            <QuerySQL queryRecord={queryRecord} />
            <QueryResultsTable isLoading={loading} rows={rows} columns={columns} />
            <ScrollBar orientation="vertical" />
            <ScrollBar orientation="horizontal" />
          </ScrollArea>
        </>
      )}
    </>
  );
}
