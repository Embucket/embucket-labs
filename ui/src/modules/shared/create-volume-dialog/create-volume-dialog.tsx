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
import type { VolumeCreatePayload } from '@/orval/models';
import { getGetVolumesQueryKey, useCreateVolume } from '@/orval/volumes';

import { CreateVolumeDialogForm } from './create-volume-dialog-form';

interface CreateVolumeDialogProps {
  opened: boolean;
  onSetOpened: (opened: boolean) => void;
}

export function CreateVolumeDialog({ opened, onSetOpened }: CreateVolumeDialogProps) {
  const queryClient = useQueryClient();
  const { mutate, isPending, error } = useCreateVolume({
    mutation: {
      onSuccess: async () => {
        await Promise.all([
          queryClient.invalidateQueries({
            queryKey: getGetDashboardQueryKey(),
          }),
          queryClient.invalidateQueries({
            queryKey: getGetVolumesQueryKey(),
          }),
        ]);
        onSetOpened(false);
        toast.success('Volume was successfully created');
      },
    },
  });

  return (
    <Dialog open={opened} onOpenChange={onSetOpened}>
      <DialogContent className="min-w-[596px]">
        <DialogHeader>
          <DialogTitle>Create Volume</DialogTitle>
        </DialogHeader>
        {error && (
          <Alert variant="destructive">
            <AlertDescription>{JSON.stringify(error.response?.data.message)}</AlertDescription>
          </Alert>
        )}
        <CreateVolumeDialogForm
          onSubmit={(formData) => {
            mutate({ data: formData as VolumeCreatePayload });
          }}
        />
        <DialogFooter>
          <Button disabled={isPending} variant="outline" onClick={() => onSetOpened(false)}>
            Cancel
          </Button>
          <Button disabled={isPending} form="createVolumeDialogForm" type="submit">
            Create
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
