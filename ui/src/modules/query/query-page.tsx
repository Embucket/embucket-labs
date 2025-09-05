import { Link, useParams } from '@tanstack/react-router';
import { ArrowLeftIcon, DatabaseZap } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { useGetQuery } from '@/orval/queries';

import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { QueryDetails } from './query-details';
import { QueryResultsTable } from './query-result-table';
import { QuerySQL } from './query-sql';
import { useMeasureQueryResultsHeight } from './use-measure-query-results-height';

export function QueryPage() {
  const { queryId } = useParams({ from: '/queries/$queryId/' });

  const { data: queryRecord, isLoading } = useGetQuery(+queryId);

  const columns = queryRecord?.result.columns ?? [];
  const rows = queryRecord?.result.rows ?? [];

  const { detailsRef, tableStyle } = useMeasureQueryResultsHeight({ isReady: !isLoading });

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
            <h1 className="text-lg">Query - {queryRecord?.id}</h1>
          </div>
        }
      />
      {!queryRecord ? (
        <PageEmptyContainer
          Icon={DatabaseZap}
          title="Query not found"
          description="The query you are looking for does not exist."
        />
      ) : (
        <>
          <div ref={detailsRef} className="p-4">
            <QueryDetails queryRecord={queryRecord} />
          </div>

          <ScrollArea tableViewport className="mx-4" style={tableStyle}>
            <QuerySQL queryRecord={queryRecord} />
            {!!rows.length && (
              <QueryResultsTable isLoading={isLoading} rows={rows} columns={columns} />
            )}
            <ScrollBar orientation="vertical" />
            <ScrollBar orientation="horizontal" />
          </ScrollArea>
        </>
      )}
    </>
  );
}
