import { Link } from '@tanstack/react-router';
import { Table } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';

import { PageContent } from '../shared/page/page-content';
import { PageHeader } from '../shared/page/page-header';

export function TablesPage() {
  return (
    <>
      <PageHeader title="Tables">
        <Link
          className="text-blue-500"
          to="/databases/$databaseName/schemas"
          params={{ databaseName: '1' }}
        >
          ← Schemas
        </Link>
        <Link
          className="text-blue-500"
          to="/databases/$databaseName/schemas/$schemaName/tables/$tableName/columns"
          params={{ databaseName: '1', schemaName: '1', tableName: '1' }}
        >
          Columns →
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
