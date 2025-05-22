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
import { getGetDatabasesQueryKey, useCreateDatabase } from '@/orval/databases';
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
          queryClient.invalidateQueries({
            queryKey: getGetDatabasesQueryKey(),
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
            <AlertDescription>{JSON.stringify(error.response?.data.message)}</AlertDescription>
          </Alert>
        )}
        <CreateDatabaseDialogForm
          volumes={volumes ?? []}
          onSubmit={({ name, volumeName }) => {
            mutate({
              data: {
                name,
                volume: volumeName,
                // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                // @ts-expect-error
                created_at: '',
                updated_at: '',
              },
            });
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
