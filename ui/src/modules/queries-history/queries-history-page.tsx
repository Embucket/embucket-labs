import { DatabaseZap } from 'lucide-react';

import { useGetQueries } from '@/orval/queries';

import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { PageScrollArea } from '../shared/page/page-scroll-area';
import { QueriesHistoryPageToolbar } from './queries-history-page-tooblar';
import { QueriesHistoryTable } from './queries-history-table';

export function QueriesHistoryPage() {
  const { data: { items: queries } = {}, isFetching } = useGetQueries();

  return (
    <>
      <PageHeader title="Queries History" />
      {!queries?.length ? (
        <PageEmptyContainer
          Icon={DatabaseZap}
          title="No Queries Found"
          description="No queries have been executed yet. Start querying data to see your history here."
        />
      ) : (
        <>
          <QueriesHistoryPageToolbar queries={queries} isFetchingQueries={isFetching} />
          <PageScrollArea>
            <QueriesHistoryTable isLoading={isFetching} queries={queries} />
          </PageScrollArea>
        </>
      )}
    </>
  );
}
