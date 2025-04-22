import { useState } from 'react';

import { ChevronRight, Database, Folder, FolderTree, MoreHorizontal, Table } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import {
  SidebarMenu,
  SidebarMenuAction,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuSub,
  SidebarMenuSubButton,
  SidebarMenuSubItem,
} from '@/components/ui/sidebar';
import { cn } from '@/lib/utils';
import type {
  NavigationTreeDatabase,
  NavigationTreeSchema,
  NavigationTreeTable,
} from '@/orval/models';

import { SqlEditorUploadDialog } from '../sql-editor-upload-dropzone/sql-editor-upload-dialog';

export interface SelectedTree {
  databaseName: string;
  schemaName: string;
  tableName: string;
}

interface DatabaseProps {
  databases: NavigationTreeDatabase[];
  selectedTree?: SelectedTree;
  onSetSelectedTree: (database: SelectedTree) => void;
  onOpenUploadDialog: () => void;
}

function Databases({
  databases,
  onSetSelectedTree,
  selectedTree,
  onOpenUploadDialog,
}: DatabaseProps) {
  // const [hoveredDatabase, setHoveredDatabase] = useState<NavigationTreeDatabase | null>(null);

  return (
    <>
      {databases.map((database, index) => (
        <SidebarMenuItem key={index}>
          <Collapsible
            className="group/collapsible [&[data-state=open]>button>svg:first-child]:rotate-90"
            defaultOpen={
              database.name === 'database1' ||
              database.schemas.some((schema) =>
                schema.tables.some((table) => table.name === selectedTree?.tableName),
              )
            }
          >
            <CollapsibleTrigger asChild>
              <SidebarMenuButton
                className="hover:bg-sidebar-secondary-accent!"
                // onMouseEnter={() => setHoveredDatabase(database)}
                // onMouseLeave={() => setHoveredDatabase(null)}
              >
                <ChevronRight className="transition-transform" />
                <Database />
                <span className="truncate">{database.name}</span>
              </SidebarMenuButton>
            </CollapsibleTrigger>
            <CollapsibleContent>
              <SidebarMenuSub className="mr-0 pr-0 pb-0">
                <Schemas
                  selectedTree={selectedTree}
                  onSetSelectedTree={onSetSelectedTree}
                  onOpenUploadDialog={onOpenUploadDialog}
                  schemas={database.schemas}
                  database={database}
                />
              </SidebarMenuSub>
            </CollapsibleContent>
          </Collapsible>
          {/* <DropdownMenu>
            <DropdownMenuTrigger asChild className={cn(hoveredDatabase === database && 'visible')}>
              <SidebarMenuAction>
                <MoreHorizontal />
              </SidebarMenuAction>
            </DropdownMenuTrigger>
            <DropdownMenuContent side="right" align="start">
              <DropdownMenuItem>
                <span>Edit Project</span>
              </DropdownMenuItem>
              <DropdownMenuItem>
                <span>Delete Project</span>
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu> */}
        </SidebarMenuItem>
      ))}
    </>
  );
}

interface SchemasProps {
  schemas: NavigationTreeSchema[];
  database: NavigationTreeDatabase;
  onSetSelectedTree: (database: SelectedTree) => void;
  onOpenUploadDialog: () => void;
  selectedTree?: SelectedTree;
}

function Schemas({
  schemas,
  database,
  onSetSelectedTree,
  selectedTree,
  onOpenUploadDialog,
}: SchemasProps) {
  // const [hoveredSchema, setHoveredSchema] = useState<NavigationTreeSchema | null>(null);

  return (
    <>
      {schemas.map((schema, index) => (
        <SidebarMenuSubItem key={index}>
          <Collapsible
            defaultOpen={schema.tables.some((table) => table.name === selectedTree?.tableName)}
            className="group/collapsible [&[data-state=open]>a>svg:first-child]:rotate-90"
          >
            <CollapsibleTrigger asChild>
              <SidebarMenuSubButton
                className="hover:bg-sidebar-secondary-accent"
                // onMouseEnter={() => setHoveredSchema(schema)}
                // onMouseLeave={() => setHoveredSchema(null)}
              >
                <ChevronRight className="transition-transform" />
                <FolderTree />
                <span className="truncate">{schema.name}</span>
              </SidebarMenuSubButton>
            </CollapsibleTrigger>
            <CollapsibleContent>
              <SidebarMenuSub className="mr-0 pr-0 pb-0">
                <Tables
                  selectedTree={selectedTree}
                  onSetSelectedTree={onSetSelectedTree}
                  onOpenUploadDialog={onOpenUploadDialog}
                  tables={schema.tables}
                  database={database}
                  schema={schema}
                />
              </SidebarMenuSub>
            </CollapsibleContent>
          </Collapsible>
          {/* <DropdownMenu>
            <DropdownMenuTrigger asChild className={cn(hoveredSchema === schema && 'visible')}>
              <SidebarMenuAction className="size-7">
                <MoreHorizontal />
              </SidebarMenuAction>
            </DropdownMenuTrigger>

            <DropdownMenuContent side="right" align="start">
              <DropdownMenuItem>
                <span>Edit Project</span>
              </DropdownMenuItem>
              <DropdownMenuItem>
                <span>Delete Project</span>
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu> */}
        </SidebarMenuSubItem>
      ))}
    </>
  );
}

interface TablesProps {
  tables: NavigationTreeTable[];
  database: NavigationTreeDatabase;
  schema: NavigationTreeSchema;
  selectedTree?: SelectedTree;
  onSetSelectedTree: (database: SelectedTree) => void;
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
    <Collapsible
      className="group/collapsible [&[data-state=open]>a>svg:first-child]:rotate-90"
      defaultOpen={tables.some((table) => table.name === selectedTree?.tableName)}
    >
      <CollapsibleTrigger asChild>
        <SidebarMenuSubButton className="hover:bg-sidebar-secondary-accent">
          <ChevronRight className="transition-transform" />
          <Folder />
          Tables
        </SidebarMenuSubButton>
      </CollapsibleTrigger>
      <CollapsibleContent>
        <SidebarMenuSub className="mr-0 pr-0 pb-0">
          {tables.map((table, index) => (
            <SidebarMenuSubItem key={index}>
              <SidebarMenuSubButton
                className="hover:bg-sidebar-secondary-accent data-[active=true]:bg-sidebar-secondary-accent!"
                isActive={selectedTree?.tableName === table.name}
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
                <span className="max-w-21 truncate">{table.name}</span>
              </SidebarMenuSubButton>
              <DropdownMenu>
                <DropdownMenuTrigger asChild className={cn(hoveredTable === table && 'visible')}>
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
        </SidebarMenuSub>
      </CollapsibleContent>
    </Collapsible>
  );
}

interface SqlEditorLeftPanelDatabasesProps {
  selectedTree?: SelectedTree;
  navigationTrees: NavigationTreeDatabase[];
  onSetSelectedTree: (database: SelectedTree) => void;
}

export function SqlEditorLeftPanelDatabases({
  navigationTrees,
  selectedTree,
  onSetSelectedTree,
}: SqlEditorLeftPanelDatabasesProps) {
  const [isDialogOpen, setIsDialogOpen] = useState(false);

  if (!navigationTrees.length) {
    return (
      <EmptyContainer
        className="absolute text-center text-wrap"
        Icon={Database}
        title="No Databases Available"
        description="Create a database to start organizing your data."
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        onCtaClick={() => {}}
        ctaText="Create database"
      />
    );
  }

  return (
    <>
      <SidebarMenu className="w-full max-w-[256px] px-4 select-none">
        <Databases
          selectedTree={selectedTree}
          onSetSelectedTree={onSetSelectedTree}
          databases={navigationTrees}
          onOpenUploadDialog={() => setIsDialogOpen(true)}
        />
      </SidebarMenu>
      {selectedTree && (
        <SqlEditorUploadDialog
          opened={isDialogOpen}
          onSetOpened={setIsDialogOpen}
          selectedTree={selectedTree}
        />
      )}
    </>
  );
}
