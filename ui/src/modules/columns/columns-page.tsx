import { Link } from '@tanstack/react-router';
import { Columns } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';

import { PageContent } from '../shared/page/page-content';
import { PageHeader } from '../shared/page/page-header';

export function ColumnsPage() {
  return (
    <>
      <PageHeader title="Columns">
        <Link
          className="text-blue-500"
          to="/databases/$databaseId/schemas/$schemaId/tables"
          params={{ databaseId: '1', schemaId: '1' }}
        >
          ← Tables
        </Link>
      </PageHeader>
      <PageContent>
        <EmptyContainer
          // TODO: Hardcode
          className="min-h-[calc(100vh-32px-65px-32px)]"
          Icon={Columns}
          title="No Columns Found"
          description="No columns have been created yet. Create a column to get started."
        />
      </PageContent>
    </>
  );
}
