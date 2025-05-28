import { useQueryClient } from '@tanstack/react-query';
import { useNavigate, useParams } from '@tanstack/react-router';
import { FolderTree, Table } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';
import { useGetTables } from '@/orval/tables';
import { getGetWorksheetsQueryKey, useCreateWorksheet } from '@/orval/worksheets';

import { DataPageTrees } from '../shared/data-page/data-page-trees';
import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { PageScrollArea } from '../shared/page/page-scroll-area';
import { useSqlEditorSettingsStore } from '../sql-editor/sql-editor-settings-store';
import { TablesTable } from './tables-page-table';
import { TablesPageToolbar } from './tables-page-toolbar';

const CREATE_TABLE_QUERY = `-- Replace <table_name> with the desired one (e.g., 's'), and specify appropriate column names and data types.
-- Example: CREATE TABLE mydb1.myschema1.s (id INT, name VARCHAR(100));
CREATE TABLE mydb1.myschema1.<table_name> (<col1_name> <col1_type>, <col2_name> <col2_type>);
`;

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
        content: CREATE_TABLE_QUERY,
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
          <PageHeader
            title={schemaName}
            Icon={FolderTree}
            Action={
              <Button size="sm" disabled={isPending} onClick={handleCreateTable}>
                Add Table
              </Button>
            }
          />
          {!tables?.length ? (
            <PageEmptyContainer
              Icon={Table}
              title="No Tables Found"
              description="No tables have been created yet. Create a table to get started."
            />
          ) : (
            <>
              <TablesPageToolbar tables={tables} isFetchingTables={isFetching} />
              <PageScrollArea>
                <TablesTable
                  isLoading={isFetching}
                  tables={tables}
                  databaseName={databaseName}
                  schemaName={schemaName}
                />
              </PageScrollArea>
            </>
          )}
        </ResizablePanel>
      </ResizablePanelGroup>
    </>
  );
}
