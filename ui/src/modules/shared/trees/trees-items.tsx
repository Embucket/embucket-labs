import type { ReactNode } from 'react';
import { useState } from 'react';

import { Database, Folder, FolderTree, Table } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import {
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuSubButton,
  SidebarMenuSubItem,
} from '@/components/ui/sidebar';
import type {
  NavigationTreeDatabase,
  NavigationTreeSchema,
  NavigationTreeTable,
} from '@/orval/models';

import { TreeCollapsibleItem } from './trees-collapsible-item';

interface TreeItemProps<T> {
  isActive?: (item: T) => boolean;
  onClick?: (item: T) => void;
}

export interface SelectedTree {
  databaseName: string;
  schemaName: string;
  tableName: string;
}

interface TreesTablesProps extends TreeItemProps<NavigationTreeTable> {
  database: NavigationTreeDatabase;
  schema: NavigationTreeSchema;
  tables: NavigationTreeTable[];
  defaultOpen?: boolean;
  renderDropdownMenu?: (tree: SelectedTree, hovered: boolean) => ReactNode;
}

export function TreesTables({
  database,
  schema,
  tables,
  onClick,
  isActive,
  renderDropdownMenu,
  defaultOpen,
}: TreesTablesProps) {
  const [hoveredTable, setHoveredTable] = useState<NavigationTreeTable>();

  return (
    <TreeCollapsibleItem
      icon={Folder}
      label="Tables"
      triggerComponent={SidebarMenuSubButton}
      defaultOpen={defaultOpen}
    >
      {tables.map((table) => {
        const tree = {
          databaseName: database.name,
          schemaName: schema.name,
          tableName: table.name,
        };
        return (
          <SidebarMenuSubItem key={table.name}>
            <SidebarMenuSubButton
              className="hover:bg-sidebar-secondary-accent data-[active=true]:bg-sidebar-secondary-accent!"
              isActive={isActive?.(table)}
              onClick={(e) => {
                e.stopPropagation();
                e.preventDefault();
                onClick?.(table);
              }}
              onMouseEnter={() => setHoveredTable(table)}
              onMouseLeave={() => setHoveredTable(undefined)}
            >
              <Table />
              <span className="truncate" title={table.name}>
                {table.name}
              </span>
            </SidebarMenuSubButton>
            {renderDropdownMenu?.(tree, hoveredTable === table)}
          </SidebarMenuSubItem>
        );
      })}
    </TreeCollapsibleItem>
  );
}

interface TreesSchemasProps extends TreeItemProps<NavigationTreeSchema> {
  schemas: NavigationTreeSchema[];
  defaultOpen?: (schema: NavigationTreeSchema) => boolean;
  children: (schema: NavigationTreeSchema) => React.ReactNode;
}

export function TreesSchemas({
  schemas,
  onClick,
  isActive,
  defaultOpen,
  children,
}: TreesSchemasProps) {
  return (
    <>
      {schemas.map((schema) => (
        <SidebarMenuSubItem key={schema.name}>
          <TreeCollapsibleItem
            icon={FolderTree}
            label={schema.name}
            triggerComponent={SidebarMenuSubButton}
            isActive={isActive?.(schema)}
            onClick={() => onClick?.(schema)}
            defaultOpen={defaultOpen?.(schema)}
          >
            {children(schema)}
          </TreeCollapsibleItem>
        </SidebarMenuSubItem>
      ))}
    </>
  );
}

interface TreesDatabasesProps extends TreeItemProps<NavigationTreeDatabase> {
  isFetchingDatabases: boolean;
  databases?: NavigationTreeDatabase[];
  defaultOpen?: (database: NavigationTreeDatabase) => boolean;
  children: (database: NavigationTreeDatabase) => React.ReactNode;
}

export function TreesDatabases({
  isFetchingDatabases,
  databases,
  isActive,
  defaultOpen,
  onClick,
  children,
}: TreesDatabasesProps) {
  if (isFetchingDatabases) {
    return null;
  }

  if (!databases?.length) {
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
      {databases.map((database) => (
        <SidebarMenuItem key={database.name}>
          <TreeCollapsibleItem
            icon={Database}
            label={database.name}
            triggerComponent={SidebarMenuButton}
            isActive={isActive?.(database)}
            defaultOpen={defaultOpen?.(database)}
            onClick={() => onClick?.(database)}
          >
            {children(database)}
          </TreeCollapsibleItem>
        </SidebarMenuItem>
      ))}
    </>
  );
}

export function TreesLayout({ children }: { children: ReactNode }) {
  return (
    <ScrollArea className="size-full py-2">
      <SidebarMenu className="w-full px-2 select-none">{children}</SidebarMenu>
      <ScrollBar orientation="vertical" />
    </ScrollArea>
  );
}
