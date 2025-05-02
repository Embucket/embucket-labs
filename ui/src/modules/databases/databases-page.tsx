import { Link } from '@tanstack/react-router';
import { Database } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';

import { PageContent } from '../shared/page/page-content';
import { PageHeader } from '../shared/page/page-header';

export function DatabasesPage() {
  return (
    <>
      <PageHeader title="Databases">
        <Link
          className="text-blue-500"
          to="/databases/$databaseId/schemas"
          params={{ databaseId: '1' }}
        >
          Schemas →
        </Link>
      </PageHeader>
      <PageContent>
        <EmptyContainer
          // TODO: Hardcode
          className="min-h-[calc(100vh-32px-65px-32px)]"
          Icon={Database}
          title="No Databases Found"
          description="No databases have been created yet. Create a database to get started."
        />
      </PageContent>
    </>
  );
}
