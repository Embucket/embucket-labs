import { t } from 'i18next';
import { Table } from 'lucide-react';
import { toast } from 'sonner';

import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { useUploadFile } from '@/orval/tables';

import { TableDataUploadDropzone } from './table-data-upload-dropzone';

interface TableDataUploadDialogProps {
  opened: boolean;
  databaseName: string;
  schemaName: string;
  tableName: string;
  onSetOpened: (opened: boolean) => void;
}

export function TableDataUploadDialog({
  opened,
  onSetOpened,
  databaseName,
  schemaName,
  tableName,
}: TableDataUploadDialogProps) {
  const { mutate, isPending } = useUploadFile({
    mutation: {
      onSuccess: () => {
        toast.success(t('successToast'));
      },
    },
  });

  const handleUpload = (file: File) => {
    mutate({
      databaseName,
      tableName,
      schemaName,
      data: {
        uploadFile: file,
      },
    });
  };

  return (
    <Dialog open={opened} onOpenChange={onSetOpened}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Load Data into Table</DialogTitle>
          <div className="text-muted-foreground mt-2 flex items-center gap-2 text-sm">
            <Table className="size-4" />
            {/* TODO: Hardcode */}
            <span className="max-w-[500px] truncate">
              {`${databaseName}.${schemaName}.${tableName}`}
            </span>
          </div>
        </DialogHeader>
        {/* {error && (
          <Alert variant="destructive">
            <AlertDescription>{JSON.stringify(error.response?.data)}</AlertDescription>
          </Alert>
        )} */}
        <TableDataUploadDropzone isDisabled={isPending} onUpload={handleUpload} />
        {/* <DialogFooter>
          <Button disabled variant="outline" onClick={() => onSetOpened(false)}>
            Cancel
          </Button>
          <Button disabled form="sqlEditorUploadDialogForm" type="submit">
            Upload
          </Button>
        </DialogFooter> */}
      </DialogContent>
    </Dialog>
  );
}
