import { Link } from '@tanstack/react-router';

import { PageContent } from '../shared/page/page-content';
import { PageHeader } from '../shared/page/page-header';
import { DatabasesPageTrees } from './databases-page-trees';

export function DatabasesPage() {
  return (
    <>
      <PageHeader title="Databases" />
      <PageContent>
        <Link to="/databases/$databaseId/schemas" params={{ databaseId: '1' }}>
          Schemas
        </Link>
        <DatabasesPageTrees />
      </PageContent>
    </>
  );
}
