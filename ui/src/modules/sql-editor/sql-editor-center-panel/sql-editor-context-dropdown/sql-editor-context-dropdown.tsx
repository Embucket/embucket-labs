import { useMemo, useState } from 'react';

import { ChevronsUpDown } from 'lucide-react';

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { SidebarMenuButton } from '@/components/ui/sidebar';
import { transformNavigationTreesToSelectOptions } from '@/modules/shared/table-data-upload-dialog/table-data-upload-utils';
import { useGetNavigationTrees } from '@/orval/navigation-trees';

import { useSqlEditorSettingsStore } from '../../sql-editor-settings-store';
import { SqlEditorContextDropdownDatabases } from './sql-editor-context-dropdown-databases';
import { SqlEditorContextDropdownSchemas } from './sql-editor-context-dropdown-schemas';
import { useSyncSqlEditorContext } from './use-sync-sql-editor-context';

export const SqlEditorContextDropdown = () => {
  const [open, setOpen] = useState(false);

  const { data: { items: navigationTrees } = {}, isLoading: isLoadingNavigationTrees } =
    useGetNavigationTrees();

  const { selectedContext, setSelectedContext } = useSqlEditorSettingsStore();
  const selectedDatabase = selectedContext.database;
  const selectedSchema = selectedContext.schema;

  const { databasesOptions, schemasOptions } = useMemo(
    () =>
      isLoadingNavigationTrees
        ? { databasesOptions: [], schemasOptions: [], tablesOptions: [] }
        : // TODO: Tmp solution
          transformNavigationTreesToSelectOptions(
            navigationTrees ?? [],
            selectedDatabase,
            selectedSchema,
          ),
    [navigationTrees, isLoadingNavigationTrees, selectedDatabase, selectedSchema],
  );

  useSyncSqlEditorContext({
    databasesOptions,
    schemasOptions,
  });

  const handleSelectDatabase = (database: string) => {
    setSelectedContext({ database, schema: schemasOptions[0].value });
  };

  const handleSelectSchema = (schema: string) => {
    setSelectedContext({ database: selectedDatabase, schema });
    setOpen(false);
  };

  return (
    <DropdownMenu open={open} onOpenChange={setOpen}>
      <DropdownMenuTrigger asChild className="h-8 w-fit max-w-[240px]">
        {selectedDatabase && selectedSchema && (
          <SidebarMenuButton
            disabled={!selectedDatabase}
            className="data-[state=open]:bg-sidebar-secondary-accent min-w-[100px]"
          >
            <p className="truncate text-sm">{`${selectedDatabase}.${selectedSchema}`}</p>
            <ChevronsUpDown className="ml-auto size-4 group-data-[collapsible=icon]:hidden" />
          </SidebarMenuButton>
        )}
      </DropdownMenuTrigger>
      <DropdownMenuContent
        className="grid w-[--radix-dropdown-menu-trigger-width] min-w-[400px] grid-cols-2 gap-0"
        side="bottom"
        align="start"
        sideOffset={4}
      >
        <SqlEditorContextDropdownDatabases
          databases={databasesOptions}
          selectedDatabase={selectedDatabase}
          onSelectDatabase={handleSelectDatabase}
        />
        <SqlEditorContextDropdownSchemas
          schemas={schemasOptions}
          selectedSchema={selectedSchema}
          onSelectSchema={handleSelectSchema}
          isDisabled={!selectedDatabase}
        />
      </DropdownMenuContent>
    </DropdownMenu>
  );
};
