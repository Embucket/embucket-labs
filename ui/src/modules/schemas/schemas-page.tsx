import { Link } from '@tanstack/react-router';

import { PageContent } from '../shared/page/page-content';
import { PageHeader } from '../shared/page/page-header';

export function SchemasPage() {
  return (
    <>
      <PageHeader title="Schemas" />
      <PageContent>
        <Link to="/databases" params={{ databaseId: '1' }}>
          Databases
        </Link>
        <Link
          to="/databases/$databaseId/schemas/$schemaId/tables"
          params={{ databaseId: '1', schemaId: '1' }}
        >
          Tables
        </Link>
      </PageContent>
    </>
  );
}
