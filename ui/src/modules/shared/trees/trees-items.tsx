import { useState } from 'react';

import { Database, Folder, FolderTree, MoreHorizontal, Table } from 'lucide-react';

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import {
  SidebarMenuAction,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuSubButton,
  SidebarMenuSubItem,
} from '@/components/ui/sidebar';
import { cn } from '@/lib/utils';
import { useSqlEditorPanelsState } from '@/modules/sql-editor/sql-editor-panels-state-provider';
import { useSqlEditorSettingsStore } from '@/modules/sql-editor/sql-editor-settings-store';
import type {
  NavigationTreeDatabase,
  NavigationTreeSchema,
  NavigationTreeTable,
} from '@/orval/models';

import { TreeCollapsibleItem } from './trees-collapsible-item';

// TODO: Need more specific name
export interface SelectedTree {
  databaseName: string;
  schemaName: string;
  tableName: string;
}

interface TablesProps {
  tables: NavigationTreeTable[];
  database: NavigationTreeDatabase;
  schema: NavigationTreeSchema;
  onOpenUploadDialog: () => void;
}

function Tables({ tables, schema, database, onOpenUploadDialog }: TablesProps) {
  const [hoveredTable, setHoveredTable] = useState<NavigationTreeTable>();
  const selectedTree = useSqlEditorSettingsStore((state) => state.selectedTree);
  const setSelectedTree = useSqlEditorSettingsStore((state) => state.setSelectedTree);

  const { isLeftBottomPanelExpanded, leftBottomRef } = useSqlEditorPanelsState();

  const handleSelectTree = (tree: SelectedTree) => {
    if (!isLeftBottomPanelExpanded) {
      leftBottomRef.current?.resize(20);
    }
    setSelectedTree(tree);
  };

  return (
    <TreeCollapsibleItem
      icon={Folder}
      label="Tables"
      triggerComponent={SidebarMenuSubButton}
      defaultOpen={tables.some((table) => table.name === selectedTree?.tableName)}
    >
      {tables.map((table, index) => (
        <SidebarMenuSubItem key={index}>
          <SidebarMenuSubButton
            className="hover:bg-sidebar-secondary-accent data-[active=true]:bg-sidebar-secondary-accent!"
            isActive={
              selectedTree?.tableName === table.name &&
              selectedTree.schemaName === schema.name &&
              selectedTree.databaseName === database.name
            }
            onClick={() =>
              handleSelectTree({
                databaseName: database.name,
                schemaName: schema.name,
                tableName: table.name,
              })
            }
            onMouseEnter={() => setHoveredTable(table)}
            onMouseLeave={() => setHoveredTable(undefined)}
          >
            <Table />
            <span className="truncate" title={table.name}>
              {table.name}
            </span>
          </SidebarMenuSubButton>
          <DropdownMenu>
            <DropdownMenuTrigger
              asChild
              className={cn(
                'invisible group-hover/subitem:visible',
                hoveredTable === table && 'visible',
              )}
            >
              <SidebarMenuAction className="size-7">
                <MoreHorizontal />
              </SidebarMenuAction>
            </DropdownMenuTrigger>
            <DropdownMenuContent side="right" align="start">
              <DropdownMenuItem
                onClick={() => {
                  handleSelectTree({
                    databaseName: database.name,
                    schemaName: schema.name,
                    tableName: table.name,
                  });
                  onOpenUploadDialog();
                }}
              >
                <span>Load data</span>
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </SidebarMenuSubItem>
      ))}
    </TreeCollapsibleItem>
  );
}

interface SchemasProps {
  schemas: NavigationTreeSchema[];
  database: NavigationTreeDatabase;

  onOpenUploadDialog: () => void;
}

function Schemas({
  schemas,
  database,

  onOpenUploadDialog,
}: SchemasProps) {
  const selectedTree = useSqlEditorSettingsStore((state) => state.selectedTree);

  return (
    <>
      {schemas.map((schema, index) => (
        <SidebarMenuSubItem key={index}>
          <TreeCollapsibleItem
            icon={FolderTree}
            label={schema.name}
            triggerComponent={SidebarMenuSubButton}
            defaultOpen={schema.tables.some((table) => table.name === selectedTree?.tableName)}
          >
            <Tables
              tables={schema.tables}
              database={database}
              schema={schema}
              onOpenUploadDialog={onOpenUploadDialog}
            />
          </TreeCollapsibleItem>
        </SidebarMenuSubItem>
      ))}
    </>
  );
}

interface DatabasesProps {
  databases: NavigationTreeDatabase[];

  onOpenUploadDialog: () => void;
}

export function TreesDatabases({ databases, onOpenUploadDialog }: DatabasesProps) {
  const selectedTree = useSqlEditorSettingsStore((state) => state.selectedTree);

  return (
    <>
      {databases.map((database, index) => (
        <SidebarMenuItem key={index}>
          <TreeCollapsibleItem
            icon={Database}
            label={database.name}
            triggerComponent={SidebarMenuButton}
            triggerClassName="hover:bg-sidebar-secondary-accent! pr-2!"
            defaultOpen={
              database.name === 'database1' ||
              database.schemas.some((schema) =>
                schema.tables.some((table) => table.name === selectedTree?.tableName),
              )
            }
          >
            <Schemas
              schemas={database.schemas}
              database={database}
              onOpenUploadDialog={onOpenUploadDialog}
            />
          </TreeCollapsibleItem>
        </SidebarMenuItem>
      ))}
    </>
  );
}
