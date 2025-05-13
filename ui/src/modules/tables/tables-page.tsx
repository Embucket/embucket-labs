import { useQueryClient } from '@tanstack/react-query';
import { useNavigate, useParams } from '@tanstack/react-router';
import { FolderTree, Table } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';
import { useGetTables } from '@/orval/tables';
import { getGetWorksheetsQueryKey, useCreateWorksheet } from '@/orval/worksheets';

import { DataPageContent } from '../shared/data-page/data-page-content';
import { DataPageHeader } from '../shared/data-page/data-page-header';
import { DataPageTrees } from '../shared/data-page/data-page-trees';
import { useSqlEditorSettingsStore } from '../sql-editor/sql-editor-settings-store';
import { TablesTable } from './tables-page-table';

export function TablesPage() {
  const navigate = useNavigate();

  const { databaseName, schemaName } = useParams({
    from: '/databases/$databaseName/schemas/$schemaName/tables/',
  });
  const { data: { items: tables } = {}, isFetching } = useGetTables(databaseName, schemaName);

  const addTab = useSqlEditorSettingsStore((state) => state.addTab);
  const setSelectedTree = useSqlEditorSettingsStore((state) => state.setSelectedTree);
  const queryClient = useQueryClient();

  const { mutateAsync, isPending } = useCreateWorksheet({
    mutation: {
      onSuccess: (worksheet) => {
        addTab(worksheet);
        setSelectedTree({
          databaseName: databaseName,
          schemaName: schemaName,
          tableName: '',
        });
        navigate({
          to: '/sql-editor/$worksheetId',
          params: {
            worksheetId: worksheet.id.toString(),
          },
        });
        queryClient.invalidateQueries({
          queryKey: getGetWorksheetsQueryKey(),
        });
      },
    },
  });

  const handleCreateTable = () => {
    mutateAsync({
      data: {
        name: '',
        content: `CREATE TABLE ${databaseName}.${schemaName}.<table_name> (<col1_name> <col1_type>, <col2_name> <col2_type>)`,
      },
    });
  };

  return (
    <>
      <ResizablePanelGroup direction="horizontal">
        <ResizablePanel collapsible defaultSize={20} minSize={20} order={1}>
          <DataPageTrees />
        </ResizablePanel>
        <ResizableHandle withHandle />
        <ResizablePanel collapsible defaultSize={20} order={1}>
          <DataPageHeader
            title={schemaName}
            Icon={FolderTree}
            secondaryText={`${tables?.length} tables found`}
            Action={
              <Button disabled={isPending} onClick={handleCreateTable}>
                Add Table
              </Button>
            }
          />
          <DataPageContent
            isEmpty={!tables?.length}
            Table={
              <TablesTable
                isLoading={isFetching}
                tables={tables ?? []}
                databaseName={databaseName}
                schemaName={schemaName}
              />
            }
            emptyStateIcon={Table}
            emptyStateTitle="No Tables Found"
            emptyStateDescription="No tables have been created yet. Create a table to get started."
          />
        </ResizablePanel>
      </ResizablePanelGroup>
    </>
  );
}
