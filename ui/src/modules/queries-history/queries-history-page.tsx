import { DatabaseZap } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { useGetQueries } from '@/orval/queries';

import { PageContent } from '../shared/page/page-content';
import { PageHeader } from '../shared/page/page-header';
import { QueriesHistoryTable } from './queries-history-table';

export function QueriesHistoryPage() {
  const { data: { items: queries } = {}, isFetching } = useGetQueries();

  return (
    <>
      <PageHeader title="Queries History" />
      <PageContent>
        <div className="flex size-full flex-col p-4">
          {queries?.length ? (
            <ScrollArea tableViewport>
              <QueriesHistoryTable queries={queries} isLoading={isFetching} />
              <ScrollBar orientation="horizontal" />
            </ScrollArea>
          ) : (
            <EmptyContainer
              Icon={DatabaseZap}
              title="No Queries Found"
              description="No queries have been executed yet. Start querying data to see your history here."
            />
          )}
        </div>
      </PageContent>
    </>
  );
}
