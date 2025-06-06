import { Link, useParams } from '@tanstack/react-router';
import { ArrowLeftIcon, DatabaseZap } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { cn } from '@/lib/utils';
import { useGetQuery } from '@/orval/queries';

import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { QueryDetails } from './query-details';
import { QueryResultsTable } from './query-result-table';
import { QuerySQL } from './query-sql';

export function QueryPage() {
  const { queryId } = useParams({ from: '/queries/$queryId/' });

  const { data: queryRecord, isLoading } = useGetQuery(+queryId);

  const columns = queryRecord?.result.columns ?? [];
  const rows = queryRecord?.result.rows ?? [];

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
        // TODO: Hardcode
        <ScrollArea className={cn('h-[calc(100vh-65px-32px-2px)]')}>
          <div className="flex size-full flex-col p-4 pt-0">
            <ScrollArea tableViewport>
              <div className="mt-4 flex flex-col gap-4">
                <QueryDetails queryRecord={queryRecord} />

                <QuerySQL queryRecord={queryRecord} />
                {!!rows.length && (
                  <QueryResultsTable isLoading={isLoading} rows={rows} columns={columns} />
                )}
              </div>
              <ScrollBar orientation="horizontal" />
            </ScrollArea>
          </div>
          <ScrollBar orientation="vertical" />
        </ScrollArea>
      )}
    </>
  );
}
