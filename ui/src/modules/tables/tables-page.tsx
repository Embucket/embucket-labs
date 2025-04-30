import { Link } from '@tanstack/react-router';

import { PageContent } from '../shared/page/page-content';
import { PageHeader } from '../shared/page/page-header';

export function TablesPage() {
  return (
    <>
      <PageHeader title="Tables" />
      <PageContent>
        <Link to="/databases/$databaseId/schemas" params={{ databaseId: '1' }}>
          Schemas
        </Link>
        <Link
          to="/databases/$databaseId/schemas/$schemaId/tables/$tableId/columns"
          params={{ databaseId: '1', schemaId: '1', tableId: '1' }}
        >
          Columns
        </Link>
      </PageContent>
    </>
  );
}
