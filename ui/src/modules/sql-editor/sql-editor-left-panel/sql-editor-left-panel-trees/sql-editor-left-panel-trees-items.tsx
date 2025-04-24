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
import type {
  NavigationTreeDatabase,
  NavigationTreeSchema,
  NavigationTreeTable,
} from '@/orval/models';

import { SqlEditorLeftPanelTreeCollapsibleItem } from './sql-editor-left-panel-trees-collapsible-item';

export interface SelectedTree {
  databaseName: string;
  schemaName: string;
  tableName: string;
}

interface TablesProps {
  tables: NavigationTreeTable[];
  database: NavigationTreeDatabase;
  schema: NavigationTreeSchema;
  selectedTree?: SelectedTree;
  onSetSelectedTree: (tree: SelectedTree) => void;
  onOpenUploadDialog: () => void;
}

function Tables({
  tables,
  schema,
  database,
  selectedTree,
  onSetSelectedTree,
  onOpenUploadDialog,
}: TablesProps) {
  const [hoveredTable, setHoveredTable] = useState<NavigationTreeTable | null>(null);

  return (
    <SqlEditorLeftPanelTreeCollapsibleItem
      icon={Folder}
      label="Tables"
      triggerComponent={SidebarMenuSubButton}
      // defaultOpen={tables.some((table) => table.name === selectedTree?.tableName)}
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
              onSetSelectedTree({
                databaseName: database.name,
                schemaName: schema.name,
                tableName: table.name,
              })
            }
            onMouseEnter={() => setHoveredTable(table)}
            onMouseLeave={() => setHoveredTable(null)}
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
                  onSetSelectedTree({
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
    </SqlEditorLeftPanelTreeCollapsibleItem>
  );
}

interface SchemasProps {
  schemas: NavigationTreeSchema[];
  database: NavigationTreeDatabase;
  selectedTree?: SelectedTree;
  onSetSelectedTree: (tree: SelectedTree) => void;
  onOpenUploadDialog: () => void;
}

function Schemas({
  schemas,
  database,
  selectedTree,
  onSetSelectedTree,
  onOpenUploadDialog,
}: SchemasProps) {
  return (
    <>
      {schemas.map((schema, index) => (
        <SidebarMenuSubItem key={index}>
          <SqlEditorLeftPanelTreeCollapsibleItem
            icon={FolderTree}
            label={schema.name}
            triggerComponent={SidebarMenuSubButton}
            // defaultOpen={schema.tables.some((table) => table.name === selectedTree?.tableName)}
          >
            <Tables
              tables={schema.tables}
              database={database}
              schema={schema}
              selectedTree={selectedTree}
              onSetSelectedTree={onSetSelectedTree}
              onOpenUploadDialog={onOpenUploadDialog}
            />
          </SqlEditorLeftPanelTreeCollapsibleItem>
        </SidebarMenuSubItem>
      ))}
    </>
  );
}

interface DatabasesProps {
  databases: NavigationTreeDatabase[];
  selectedTree?: SelectedTree;
  onSetSelectedTree: (tree: SelectedTree) => void;
  onOpenUploadDialog: () => void;
}

export function SqlEditorLeftPanelTreesDatabases({
  databases,
  selectedTree,
  onSetSelectedTree,
  onOpenUploadDialog,
}: DatabasesProps) {
  return (
    <>
      {databases.map((database, index) => (
        <SidebarMenuItem key={index}>
          <SqlEditorLeftPanelTreeCollapsibleItem
            icon={Database}
            label={database.name}
            triggerComponent={SidebarMenuButton}
            triggerClassName="hover:bg-sidebar-secondary-accent! pr-2!"
            // defaultOpen={
            //   database.name === 'database1' ||
            //   database.schemas.some((schema) =>
            //     schema.tables.some((table) => table.name === selectedTree?.tableName),
            //   )
            // }
          >
            <Schemas
              schemas={database.schemas}
              database={database}
              selectedTree={selectedTree}
              onSetSelectedTree={onSetSelectedTree}
              onOpenUploadDialog={onOpenUploadDialog}
            />
          </SqlEditorLeftPanelTreeCollapsibleItem>
        </SidebarMenuItem>
      ))}
    </>
  );
}
