import { useState } from 'react';

import { useQueryClient } from '@tanstack/react-query';
import { useNavigate, useParams } from '@tanstack/react-router';
import { FolderTree, Table } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { useDebounce } from '@/hooks/use-debounce';
import { useGetTables } from '@/orval/tables';
import { getGetWorksheetsQueryKey, useCreateWorksheet } from '@/orval/worksheets';

import { useEditorSettingsStore } from '../editor/editor-settings-store';
import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { PageScrollArea } from '../shared/page/page-scroll-area';
import { TablesTable } from './tables-page-table';
import { TablesPageToolbar } from './tables-page-toolbar';

const CREATE_TABLE_QUERY = `-- Replace <table_name> with the desired one (e.g., 's'), and specify appropriate column names and data types.
-- Example: CREATE TABLE mydb1.myschema1.s (id INT, name VARCHAR(100));
CREATE TABLE mydb1.myschema1.<table_name> (<col1_name> <col1_type>, <col2_name> <col2_type>);
`;

export function TablesPage() {
  const navigate = useNavigate();
  const [search, setSearch] = useState('');
  const debouncedSearch = useDebounce(search, 300);

  const { databaseName, schemaName } = useParams({
    from: '/databases/_dataPagesLayout/$databaseName/schemas/$schemaName/tables/',
  });
  const {
    data: { items: tables } = {},
    isFetching: isFetchingTables,
    isLoading: isLoadingTables,
    refetch: refetchTables,
  } = useGetTables(databaseName, schemaName, {
    search: debouncedSearch,
  });

  const isTablesEmpty = !tables?.length;
  const isTablesEmptyDueToSearch = isTablesEmpty && search.length > 0;

  const addTab = useEditorSettingsStore((state) => state.addTab);
  const setSelectedTree = useEditorSettingsStore((state) => state.setSelectedTree);
  const queryClient = useQueryClient();

  const { mutate, isPending: isPendingCreateWorksheet } = useCreateWorksheet({
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
    mutate({
      data: {
        name: '',
        content: CREATE_TABLE_QUERY,
      },
    });
  };

  return (
    <>
      <PageHeader
        title={schemaName}
        Icon={FolderTree}
        Action={
          <Button size="sm" disabled={isPendingCreateWorksheet} onClick={handleCreateTable}>
            Add Table
          </Button>
        }
      />

      <TablesPageToolbar
        search={search}
        onSetSearch={setSearch}
        tables={tables ?? []}
        isFetchingTables={isFetchingTables}
        onRefetchTables={refetchTables}
      />
      {isTablesEmpty && !isLoadingTables ? (
        <PageEmptyContainer
          Icon={Table}
          variant="toolbar"
          title="No Tables Found"
          description={
            isTablesEmptyDueToSearch
              ? 'No tables match your search.'
              : 'No tables have been created yet. Create a table to get started.'
          }
        />
      ) : (
        <PageScrollArea>
          <TablesTable
            isLoading={isLoadingTables}
            tables={tables ?? []}
            databaseName={databaseName}
            schemaName={schemaName}
          />
        </PageScrollArea>
      )}
    </>
  );
}
