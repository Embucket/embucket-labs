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
import { getGetNavigationTreesQueryKey } from '@/orval/navigation-trees';
import { getGetSchemasQueryKey, useCreateSchema } from '@/orval/schemas';

import { CreateSchemaDialogForm } from './create-schema-dialog-form';

interface CreateSchemaDialogProps {
  opened: boolean;
  databaseName: string;
  onSetOpened: (opened: boolean) => void;
}

export function CreateSchemaDialog({ opened, onSetOpened, databaseName }: CreateSchemaDialogProps) {
  const queryClient = useQueryClient();
  const { mutate, isPending, error } = useCreateSchema({
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
            queryKey: getGetSchemasQueryKey(databaseName),
          }),
        ]);
        onSetOpened(false);
        toast.success('Schema was successfully created');
      },
    },
  });

  return (
    <Dialog open={opened} onOpenChange={onSetOpened}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Create Schema</DialogTitle>
        </DialogHeader>
        {error && (
          <Alert variant="destructive">
            <AlertDescription>{JSON.stringify(error.response?.data)}</AlertDescription>
          </Alert>
        )}
        <CreateSchemaDialogForm
          onSubmit={(formData) => {
            mutate({ databaseName, data: { name: formData.name } });
          }}
        />
        <DialogFooter>
          <Button disabled={isPending} variant="outline" onClick={() => onSetOpened(false)}>
            Cancel
          </Button>
          <Button disabled={isPending} form="createSchemaDialogForm" type="submit">
            Create
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
