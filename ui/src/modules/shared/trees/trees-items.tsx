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
import { cn } from '@/lib/utils';
import type {
  NavigationTreeDatabase,
  NavigationTreeSchema,
  NavigationTreeTable,
} from '@/orval/models';
import { useGetNavigationTrees } from '@/orval/navigation-trees';

import { CreateDatabaseDialog } from '../create-database-dialog/create-database-dialog';
import { TreeCollapsibleItem } from './trees-collapsible-item';
import { TreesToolbar } from './trees-toolbar';

interface TreeItemProps<T> {
  isActive?: (item: T) => boolean;
  onClick?: (item: T) => void;
  open?: boolean;
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
  label: string;
  renderDropdownMenu?: (tree: SelectedTree, hovered: boolean) => ReactNode;
}

export function TreesTables({
  database,
  schema,
  tables,
  label,
  onClick,
  isActive,
  renderDropdownMenu,
  defaultOpen,
  open,
}: TreesTablesProps) {
  const [hoveredTable, setHoveredTable] = useState<NavigationTreeTable>();

  return (
    <TreeCollapsibleItem
      icon={Folder}
      label={label}
      triggerComponent={SidebarMenuSubButton}
      defaultOpen={defaultOpen}
      open={open}
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
              onClick={() => {
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
  open,
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
            open={open}
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
  open,
  children,
}: TreesDatabasesProps) {
  const [opened, setOpened] = useState(false);

  if (isFetchingDatabases) {
    return null;
  }

  if (!databases?.length) {
    return (
      <>
        <EmptyContainer
          className="absolute text-center text-wrap"
          Icon={Database}
          title="No Databases Available"
          description="Create a database to start organizing your data."
          onCtaClick={() => setOpened(true)}
          ctaText="Create database"
        />
        <CreateDatabaseDialog opened={opened} onSetOpened={setOpened} />
      </>
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
            open={open}
          >
            {children(database)}
          </TreeCollapsibleItem>
        </SidebarMenuItem>
      ))}
    </>
  );
}

interface TreesLayoutProps {
  children: ReactNode;
  scrollAreaClassName?: string;
}

export function TreesLayout({ children, scrollAreaClassName }: TreesLayoutProps) {
  const { refetch: refetchNavigationTrees, isFetching: isFetchingNavigationTrees } =
    useGetNavigationTrees();

  return (
    <>
      <TreesToolbar
        isFetchingNavigationTrees={isFetchingNavigationTrees}
        onRefetchNavigationTrees={refetchNavigationTrees}
      />
      <ScrollArea className={cn('py-2', scrollAreaClassName)}>
        <SidebarMenu className="w-full px-2 select-none">{children}</SidebarMenu>
        <ScrollBar orientation="vertical" />
      </ScrollArea>
    </>
  );
}
