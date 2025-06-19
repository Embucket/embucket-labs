import { DatabaseZap } from 'lucide-react';

import { useGetQueries } from '@/orval/queries';

import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { PageScrollArea } from '../shared/page/page-scroll-area';
import { QueriesHistoryPageToolbar } from './queries-history-page-toolbar';
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

      <QueriesHistoryPageToolbar queries={queries ?? []} isFetchingQueries={isFetchingQueries} />
      {!queries?.length && !isLoadingQueries ? (
        <PageEmptyContainer
          Icon={DatabaseZap}
          variant="toolbar"
          title="No Queries Found"
          description="No queries have been executed yet. Start querying data to see your history here."
        />
      ) : (
        <PageScrollArea>
          <QueriesHistoryTable isLoading={isLoadingQueries} queries={queries ?? []} />
        </PageScrollArea>
      )}
    </>
  );
}
