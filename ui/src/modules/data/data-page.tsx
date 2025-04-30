import { PageContent } from '../shared/page/page-content';
import { PageHeader } from '../shared/page/page-header';
import { DataPageTrees } from './data-page-trees';

export function DataPage() {
  return (
    <>
      <PageHeader title="Data" />
      <PageContent>
        <DataPageTrees />
      </PageContent>
    </>
  );
}
