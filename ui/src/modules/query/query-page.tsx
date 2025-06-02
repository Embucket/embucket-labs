import { Link } from '@tanstack/react-router';
import { ArrowLeftIcon, DatabaseZap } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { QUERY_RECORDS_MOCK } from '@/mocks/query-records-mock';
import type { QueryRecord } from '@/orval/models';

import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { PageScrollArea } from '../shared/page/page-scroll-area';
import { QueryDetails } from './query-details';
import { QueryResultsTable } from './query-result-table';
import { QuerySQL } from './query-sql';

const QUERY_RECORD: QueryRecord | undefined = QUERY_RECORDS_MOCK[0];

export function QueryPage() {
  const columns = QUERY_RECORD?.result.columns ?? [];
  const rows = QUERY_RECORD?.result.rows ?? [];

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
            <h1 className="text-lg">Query - {QUERY_RECORD?.id}</h1>
          </div>
        }
      />
      {!QUERY_RECORD ? (
        <PageEmptyContainer
          Icon={DatabaseZap}
          title="Query not found"
          description="The query you are looking for does not exist."
        />
      ) : (
        <>
          <PageScrollArea>
            <div className="mt-4 flex flex-col gap-4">
              <QueryDetails queryRecord={QUERY_RECORD} />

              <QuerySQL queryRecord={QUERY_RECORD} />

              <QueryResultsTable isLoading={false} rows={rows} columns={columns} />
            </div>
          </PageScrollArea>
        </>
      )}
    </>
  );
}
