import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';

import { Alert, AlertDescription } from '@/components/ui/alert';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { getGetDashboardQueryKey } from '@/orval/dashboard';
import { useCreateDatabase } from '@/orval/databases';
import { getGetNavigationTreesQueryKey } from '@/orval/navigation-trees';
import { useGetVolumes } from '@/orval/volumes';

import { CreateDatabaseDialogForm } from './create-database-dialog-form';

interface CreateDatabaseDialogProps {
  opened: boolean;
  onSetOpened: (opened: boolean) => void;
}

export function CreateDatabaseDialog({ opened, onSetOpened }: CreateDatabaseDialogProps) {
  const { data: { items: volumes } = {} } = useGetVolumes();

  const queryClient = useQueryClient();
  const { mutate, isPending, error } = useCreateDatabase({
    mutation: {
      onSuccess: async () => {
        await Promise.all([
          queryClient.invalidateQueries({
            queryKey: getGetNavigationTreesQueryKey(),
          }),
          queryClient.invalidateQueries({
            queryKey: getGetDashboardQueryKey(),
          }),
        ]);
        onSetOpened(false);
        toast.success('Database was successfully created');
      },
    },
  });

  return (
    <Dialog open={opened} onOpenChange={onSetOpened}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Create Database</DialogTitle>
        </DialogHeader>
        {error && (
          <Alert variant="destructive">
            <AlertDescription>{JSON.stringify(error.response?.data)}</AlertDescription>
          </Alert>
        )}
        <CreateDatabaseDialogForm
          onSubmit={(formData) => {
            mutate({ data: { name: formData.name, volume: volumes?.[0]?.name ?? '' } });
          }}
        />
        <DialogFooter>
          <Button disabled={isPending} variant="outline" onClick={() => onSetOpened(false)}>
            Cancel
          </Button>
          <Button disabled={isPending} form="createDatabaseDialogForm" type="submit">
            Create
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
