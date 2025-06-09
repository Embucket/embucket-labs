import type { ReactNode } from 'react';
import { useState } from 'react';

import { keepPreviousData } from '@tanstack/react-query';
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
import { useGetVolumes } from '@/orval/volumes';

import { CreateDatabaseDialog } from '../create-database-dialog/create-database-dialog';
import { CreateVolumeDialog } from '../create-volume-dialog/create-volume-dialog';
import { TreeCollapsibleItem } from './trees-collapsible-item';
import { TreesSkeleton } from './trees-skeleton';
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
  databases,
  isFetchingDatabases,
  isActive,
  defaultOpen,
  onClick,
  open,
  children,
}: TreesDatabasesProps) {
  const { data: { items: volumes } = {}, isFetching: isFetchingVolumes } = useGetVolumes();

  const [createVolumeDialogOpened, setCreateVolumeDialogOpened] = useState(false);
  const [createDatabaseDialogOpened, setCreateDatabaseDialogOpened] = useState(false);

  if (isFetchingVolumes || isFetchingDatabases) {
    return <TreesSkeleton />;
  }

  // TODO: Not the best place to put empty states for volumes
  if (!volumes?.length) {
    return (
      <>
        <EmptyContainer
          className="absolute text-center text-wrap"
          Icon={Database}
          title="No Volumes Available"
          description="Create a volume to proceed with the database creation."
          onCtaClick={() => setCreateVolumeDialogOpened(true)}
          ctaText="Create volume"
        />
        <CreateVolumeDialog
          opened={createVolumeDialogOpened}
          onSetOpened={setCreateVolumeDialogOpened}
        />
      </>
    );
  }

  if (!databases?.length) {
    return (
      <>
        <EmptyContainer
          className="absolute text-center text-wrap"
          Icon={Database}
          title="No Databases Available"
          description="Create a database to get started."
          onCtaClick={() => setCreateDatabaseDialogOpened(true)}
          ctaText="Create database"
        />
        <CreateDatabaseDialog
          opened={createDatabaseDialogOpened}
          onSetOpened={setCreateDatabaseDialogOpened}
        />
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
  const {
    refetch: refetchNavigationTrees,
    isFetching: isFetchingNavigationTrees,
    isLoading: isLoadingNavigationTrees,
  } = useGetNavigationTrees({}, { query: { placeholderData: keepPreviousData } });

  return (
    <>
      <TreesToolbar
        isFetchingNavigationTrees={isFetchingNavigationTrees}
        onRefetchNavigationTrees={refetchNavigationTrees}
      />
      <ScrollArea className={cn('py-2', scrollAreaClassName)}>
        <SidebarMenu className="w-full px-2 select-none">
          {isLoadingNavigationTrees ? <TreesSkeleton /> : children}
        </SidebarMenu>
        <ScrollBar orientation="vertical" />
      </ScrollArea>
    </>
  );
}
