import { Link } from '@tanstack/react-router';
import { FolderTree } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';

import { PageContent } from '../shared/page/page-content';
import { PageHeader } from '../shared/page/page-header';

export function SchemasPage() {
  return (
    <>
      <PageHeader title="Schemas">
        <Link className="text-blue-500" to="/databases" params={{ databaseId: '1' }}>
          ← Databases
        </Link>
        <Link
          className="text-blue-500"
          to="/databases/$databaseId/schemas/$schemaId/tables"
          params={{ databaseId: '1', schemaId: '1' }}
        >
          Tables →
        </Link>
      </PageHeader>
      <PageContent>
        <EmptyContainer
          // TODO: Hardcode
          className="min-h-[calc(100vh-32px-65px-32px)]"
          Icon={FolderTree}
          title="No Schemas Found"
          description="No schemas have been created yet. Create a schema to get started."
        />
      </PageContent>
    </>
  );
}
