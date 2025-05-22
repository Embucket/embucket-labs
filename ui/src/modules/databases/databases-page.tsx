import { useState } from 'react';

import { Database } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';
import { useGetDatabases } from '@/orval/databases';
import { useGetVolumes } from '@/orval/volumes';

import { CreateDatabaseDialog } from '../shared/create-database-dialog/create-database-dialog';
import { DataPageContent } from '../shared/data-page/data-page-content';
import { DataPageHeader } from '../shared/data-page/data-page-header';
import { DataPageTrees } from '../shared/data-page/data-page-trees';
import { DatabasesTable } from './databases-page-table';

export function DatabasesPage() {
  const [opened, setOpened] = useState(false);
  const { data: { items: databases } = {}, isFetching: isFetchingDatabases } = useGetDatabases();
  const { data: { items: volumes } = {}, isFetching: isFetchingVolumes } = useGetVolumes();

  return (
    <>
      <ResizablePanelGroup direction="horizontal">
        <ResizablePanel collapsible defaultSize={20} minSize={20} order={1}>
          <DataPageTrees />
        </ResizablePanel>
        <ResizableHandle withHandle />
        <ResizablePanel collapsible defaultSize={20} order={1}>
          <DataPageHeader
            title="Databases"
            Icon={Database}
            secondaryText={`${databases?.length} databases found`}
            Action={
              <Button
                disabled={isFetchingDatabases || isFetchingVolumes || !volumes?.length}
                onClick={() => setOpened(true)}
              >
                Add Database
              </Button>
            }
          />
          <DataPageContent
            isEmpty={!databases?.length}
            Table={<DatabasesTable isLoading={isFetchingDatabases} databases={databases ?? []} />}
            emptyStateIcon={Database}
            emptyStateTitle="No Databases Found"
            emptyStateDescription="No databases have been created yet. Create a database to get started."
          />
        </ResizablePanel>
      </ResizablePanelGroup>
      <CreateDatabaseDialog opened={opened} onSetOpened={setOpened} />
    </>
  );
}
