import { useState } from 'react';

import { Box } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { useGetVolumes } from '@/orval/volumes';

import { CreateVolumeDialog } from '../shared/create-volume-dialog/create-volume-dialog';
import { DataPageContent } from '../shared/data-page/data-page-content';
import { DataPageHeader } from '../shared/data-page/data-page-header';
import { VolumesTable } from './volumes-page-table';
import { VolumesPageToolbar } from './volumes-page-toolbar';

// TODO: Not a data page
export function VolumesPage() {
  const [opened, setOpened] = useState(false);

  const { data: { items: volumes } = {}, isFetching } = useGetVolumes();

  return (
    <>
      <DataPageHeader
        title="Volumes"
        Action={
          <Button disabled={isFetching} onClick={() => setOpened(true)}>
            Add Volume
          </Button>
        }
      />
      <DataPageContent
        isEmpty={!volumes?.length}
        emptyStateIcon={Box}
        emptyStateTitle="No Volumes Found"
        emptyStateDescription="No volumes have been created yet. Create a volume to get started."
      >
        <VolumesPageToolbar volumes={volumes ?? []} isFetchingVolumes={isFetching} />
        <VolumesTable volumes={volumes ?? []} isLoading={isFetching} />
      </DataPageContent>
      <CreateVolumeDialog opened={opened} onSetOpened={setOpened} />
    </>
  );
}
