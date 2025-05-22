import { useEffect, useMemo } from 'react';

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

export const SqlEditorContextDropdown = () => {
  const { data: { items: navigationTrees } = {}, isLoading: isLoadingNavigationTrees } =
    useGetNavigationTrees();

  const { selectedContext, setSelectedContext } = useSqlEditorSettingsStore();
  const selectedDatabase = selectedContext.databaseName;
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

  const handleSelectDatabase = (databaseName: string) => {
    setSelectedContext({ databaseName, schema: schemasOptions[0].value });
  };

  const handleSelectSchema = (schema: string) => {
    setSelectedContext({ databaseName: selectedDatabase, schema });
  };

  // If no database or schema is selected, set the first database and schema
  useEffect(() => {
    if (databasesOptions.length && schemasOptions.length && !selectedDatabase && !selectedSchema) {
      setSelectedContext({
        databaseName: databasesOptions[0].value,
        schema: schemasOptions[0].value,
      });
    }
  }, [selectedDatabase, selectedSchema, setSelectedContext, databasesOptions, schemasOptions]);

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild className="h-8 w-fit max-w-[240px]">
        <SidebarMenuButton className="data-[state=open]:bg-sidebar-secondary-accent">
          <p className="truncate text-sm">{`${selectedDatabase}.${selectedSchema}`}</p>
          <ChevronsUpDown className="ml-auto size-4 group-data-[collapsible=icon]:hidden" />
        </SidebarMenuButton>
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
