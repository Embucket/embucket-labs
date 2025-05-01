import { Link } from '@tanstack/react-router';
import { Table } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';

import { PageContent } from '../shared/page/page-content';
import { PageHeader } from '../shared/page/page-header';

export function TablesPage() {
  return (
    <>
      <PageHeader title="Tables">
        <Link to="/databases/$databaseId/schemas" params={{ databaseId: '1' }}>
          Schemas
        </Link>
        <Link
          to="/databases/$databaseId/schemas/$schemaId/tables/$tableId/columns"
          params={{ databaseId: '1', schemaId: '1', tableId: '1' }}
        >
          Columns
        </Link>
      </PageHeader>
      <PageContent>
        <EmptyContainer
          // TODO: Hardcode
          className="min-h-[calc(100vh-32px-65px-32px)]"
          Icon={Table}
          title="No Tables Found"
          description="No tables have been created yet. Create a table to get started."
        />
      </PageContent>
    </>
  );
}
