import { DatabaseZap } from 'lucide-react';

import { useGetQueries } from '@/orval/queries';

import { DataPageEmptyContainer } from '../shared/data-page/data-page-empty-container';
import { DataPageHeader } from '../shared/data-page/data-page-header';
import { DataPageScrollArea } from '../shared/data-page/data-page-scroll-area';
import { QueriesHistoryPageToolbar } from './queries-history-page-tooblar';
import { QueriesHistoryTable } from './queries-history-table';

export function QueriesHistoryPage() {
  const { data: { items: queries } = {}, isFetching } = useGetQueries();

  return (
    <>
      <DataPageHeader title="Queries History" />
      {!queries?.length ? (
        <DataPageEmptyContainer
          Icon={DatabaseZap}
          title="No Queries Found"
          description="No queries have been executed yet. Start querying data to see your history here."
        />
      ) : (
        <>
          <QueriesHistoryPageToolbar queries={queries} isFetchingQueries={isFetching} />
          <DataPageScrollArea>
            <QueriesHistoryTable isLoading={isFetching} queries={queries} />
          </DataPageScrollArea>
        </>
      )}
      {/* <PageHeader title="Queries History" />
      <ScrollArea className="h-[calc(100vh-65px-32px)] p-4">
        <div className="flex size-full flex-col">
          {queries?.length ? (
            <ScrollArea tableViewport>
              <QueriesHistoryTable queries={queries} isLoading={isFetching} />
              <ScrollBar orientation="horizontal" />
            </ScrollArea>
          ) : (
            <EmptyContainer
              // TODO: Hardcode
              className="min-h-[calc(100vh-32px-65px-32px)]"
              Icon={DatabaseZap}
              title="No Queries Found"
              description="No queries have been executed yet. Start querying data to see your history here."
            />
          )}
        </div>
        <ScrollBar orientation="vertical" />
      </ScrollArea> */}
    </>
  );
}
