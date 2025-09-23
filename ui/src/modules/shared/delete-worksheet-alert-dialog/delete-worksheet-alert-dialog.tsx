import { useQueryClient } from '@tanstack/react-query';
import { useNavigate } from '@tanstack/react-router';
import { toast } from 'sonner';

import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import { useEditorSettingsStore } from '@/modules/editor/editor-settings-store';
import type { WorksheetsResponse } from '@/orval/models';
import { getGetWorksheetsQueryKey, useDeleteWorksheet } from '@/orval/worksheets';

interface DeleteWorksheetAlertDialogProps {
  worksheetId: number;
  opened: boolean;
  onSetOpened: (opened: boolean) => void;
}

export const DeleteWorksheetAlertDialog = ({
  worksheetId,
  opened,
  onSetOpened,
}: DeleteWorksheetAlertDialogProps) => {
  const addTab = useEditorSettingsStore((state) => state.addTab);
  const removeTab = useEditorSettingsStore((state) => state.removeTab);
  const navigate = useNavigate();

  const queryClient = useQueryClient();

  const { mutate, isPending } = useDeleteWorksheet({
    mutation: {
      onSuccess: async () => {
        removeTab(worksheetId);

        await queryClient.invalidateQueries({
          queryKey: getGetWorksheetsQueryKey(),
        });
        onSetOpened(false);
        toast.success('Worksheet deleted successfully');
        // TODO: Centralize logic for navigating and managing tabs
        const data = queryClient.getQueryData<WorksheetsResponse>(getGetWorksheetsQueryKey());
        const nextWorksheet = data?.items.find((w) => w.id !== worksheetId);
        if (!nextWorksheet) {
          navigate({
            to: '/home',
          });
          return;
        }
        addTab(nextWorksheet);
        navigate({
          to: '/sql-editor/$worksheetId',
          params: {
            worksheetId: nextWorksheet.id.toString() || '',
          },
        });
      },
    },
  });

  const handleSubmit = () => {
    mutate({
      worksheetId,
    });
  };

  return (
    <AlertDialog open={opened} onOpenChange={onSetOpened}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>Delete worksheet</AlertDialogTitle>
          <AlertDialogDescription>
            Are you sure you want to delete this worksheet? This action cannot be undone.
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>Cancel</AlertDialogCancel>
          <AlertDialogAction onClick={handleSubmit} disabled={isPending} variant="destructive">
            Delete
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
};
