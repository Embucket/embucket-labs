import { useState } from 'react';

import { Box } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { useGetVolumes } from '@/orval/volumes';

import { DataPageContent } from '../shared/data-page/data-page-content';
import { DataPageHeader } from '../shared/data-page/data-page-header';
import { CreateVolumeDialog } from './create-volume-dialog/create-volume-dialog';
import { VolumesTable } from './volumes-page-table';

// TODO: Not a data page
export function VolumesPage() {
  const [opened, setOpened] = useState(false);

  const { data: { items: volumes } = {}, isFetching } = useGetVolumes();

  return (
    <>
      <DataPageHeader
        title="Volumes"
        secondaryText={`${volumes?.length} volumes found`}
        Action={
          <Button disabled={isFetching} onClick={() => setOpened(true)}>
            Add Volume
          </Button>
        }
      />
      <DataPageContent
        isEmpty={!volumes?.length}
        Table={<VolumesTable volumes={volumes ?? []} isLoading={isFetching} />}
        emptyStateIcon={Box}
        emptyStateTitle="No Volumes Found"
        emptyStateDescription="No volumes have been created yet. Create a volume to get started."
      />
      <CreateVolumeDialog opened={opened} onSetOpened={setOpened} />
    </>
  );
}
