import { DatabaseZap } from 'lucide-react';

import { useGetQueries } from '@/orval/queries';

import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { PageScrollArea } from '../shared/page/page-scroll-area';
import { QueriesHistoryPageToolbar } from './queries-history-page-tooblar';
import { QueriesHistoryTable } from './queries-history-table';

export function QueriesHistoryPage() {
  const {
    data: { items: queries } = {},
    isFetching: isFetchingQueries,
    isLoading: isLoadingQueries,
  } = useGetQueries();

  return (
    <>
      <PageHeader title="Queries History" />
      {!queries?.length && !isLoadingQueries ? (
        <PageEmptyContainer
          Icon={DatabaseZap}
          title="No Queries Found"
          description="No queries have been executed yet. Start querying data to see your history here."
        />
      ) : (
        <>
          <QueriesHistoryPageToolbar
            queries={queries ?? []}
            isFetchingQueries={isFetchingQueries}
          />
          <PageScrollArea>
            <QueriesHistoryTable isLoading={isLoadingQueries} queries={queries ?? []} />
          </PageScrollArea>
        </>
      )}
    </>
  );
}
