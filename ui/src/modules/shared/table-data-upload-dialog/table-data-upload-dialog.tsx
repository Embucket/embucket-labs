import { useMemo, useState } from 'react';

import { useQueryClient } from '@tanstack/react-query';
import { Table } from 'lucide-react';
import { toast } from 'sonner';

import { Alert, AlertDescription } from '@/components/ui/alert';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { getGetDashboardQueryKey } from '@/orval/dashboard';
import { getGetDatabasesQueryKey } from '@/orval/databases';
import { getGetNavigationTreesQueryKey, useGetNavigationTrees } from '@/orval/navigation-trees';
import { useUploadFile } from '@/orval/tables';

import type { SelectedTree } from '../trees/trees-items';
import { TableDataUploadSelect } from './table-data-upload-database-select';
import { TableDataUploadDropzone } from './table-data-upload-dropzone';
import { transformNavigationTreesToSelectOptions } from './table-data-upload-utils';

interface TableDataUploadDialogProps {
  opened: boolean;
  databaseName?: string;
  schemaName?: string;
  tableName?: string;
  onSetOpened: (opened: boolean) => void;
}

// TODO: Double check keys hack
export function TableDataUploadDialog({
  opened,
  onSetOpened,
  databaseName,
  schemaName,
  tableName,
}: TableDataUploadDialogProps) {
  const { data: { items: navigationTrees } = {}, isLoading: isLoadingNavigationTrees } =
    useGetNavigationTrees();

  // TODO: Better not to reuse trees interface here
  const [tree, setTree] = useState<SelectedTree>({
    databaseName: databaseName ?? '',
    schemaName: schemaName ?? '',
    tableName: tableName ?? '',
  });

  const queryClient = useQueryClient();

  const { mutate, isPending, error } = useUploadFile({
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
        toast.success('File uploaded successfully');
        onSetOpened(false);
      },
    },
  });

  const handleUpload = (fileToUpload: File) => {
    mutate({
      databaseName: tree.databaseName,
      tableName: tree.tableName,
      schemaName: tree.schemaName,
      data: {
        uploadFile: fileToUpload,
      },
    });
  };

  const handleCreateNewTable = () => {
    setTree((prev) => ({
      ...prev,
      tableName: `table${Math.random().toString(36).substring(2, 15)}`,
    }));
  };

  const { databasesOptions, schemasOptions, tablesOptions } = useMemo(
    () =>
      isLoadingNavigationTrees
        ? { databasesOptions: [], schemasOptions: [], tablesOptions: [] }
        : transformNavigationTreesToSelectOptions(
            navigationTrees ?? [],
            tree.databaseName,
            tree.schemaName,
          ),
    [navigationTrees, isLoadingNavigationTrees, tree.databaseName, tree.schemaName],
  );

  return (
    <Dialog open={opened} onOpenChange={onSetOpened}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Load Data</DialogTitle>
          {tree.databaseName && tree.schemaName && tree.tableName && (
            <div className="text-muted-foreground mt-2 flex items-center gap-2 text-sm">
              <Table className="size-4" />
              {/* TODO: Hardcode */}
              <span className="max-w-[500px] truncate">
                {`${tree.databaseName}.${tree.schemaName}.${tree.tableName}`}
              </span>
            </div>
          )}
        </DialogHeader>
        {error && (
          <Alert variant="destructive">
            <AlertDescription>{JSON.stringify(error.response?.data.message)}</AlertDescription>
          </Alert>
        )}
        <TableDataUploadDropzone
          isDisabled={tree.tableName ? isPending : isPending || !tree.tableName}
          onUpload={handleUpload}
        />
        {!(databaseName && schemaName && tableName) && (
          <div className="flex flex-col gap-2">
            <p className="text-sm">Select database, schema and table before uploading</p>
            <div className="flex gap-2">
              <TableDataUploadSelect
                key={tree.databaseName}
                options={databasesOptions}
                value={tree.databaseName}
                onChange={(newDbName) =>
                  setTree((prev) => ({
                    ...prev,
                    databaseName: newDbName,
                    schemaName: '',
                    tableName: '',
                  }))
                }
                placeholder="Select Database"
                disabled={isPending}
              />
              <TableDataUploadSelect
                key={`${tree.databaseName}-${tree.schemaName}`}
                options={schemasOptions}
                value={tree.schemaName}
                onChange={(newSchemaName) =>
                  setTree((prev) => ({
                    ...prev,
                    schemaName: newSchemaName,
                    tableName: '',
                  }))
                }
                placeholder="Select Schema"
                disabled={isPending || !tree.databaseName}
              />
              <TableDataUploadSelect
                key={`key-${tree.schemaName}`}
                options={tablesOptions}
                value={tree.tableName}
                onChange={(tableName) => setTree((prev) => ({ ...prev, tableName }))}
                placeholder="Select Table"
                disabled={isPending || !tree.schemaName}
                customOptionLabel="Random Name (New Table)"
                onCustomOptionClick={handleCreateNewTable}
              />
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
