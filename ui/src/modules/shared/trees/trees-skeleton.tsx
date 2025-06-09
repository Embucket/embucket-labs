import { Database, FolderTree, Table } from 'lucide-react';

import {
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuSubButton,
  SidebarMenuSubItem,
} from '@/components/ui/sidebar';
import { Skeleton } from '@/components/ui/skeleton';

import { TreeCollapsibleItem } from './trees-collapsible-item';

const SkeletonRow = () => {
  return <Skeleton className="h-5 w-full" />;
};

const DATABASES = [
  { schemas: [] },
  {
    schemas: [
      {
        tables: 3,
      },
    ],
  },
  { schemas: [] },
  {
    schemas: [
      {
        tables: 1,
      },
    ],
  },
  {
    schemas: [
      {
        tables: 2,
      },
    ],
  },
];

export const TreesSkeleton = () => (
  <>
    {DATABASES.map((database, dbIndex) => (
      <SidebarMenuItem key={`db-${dbIndex}`}>
        <TreeCollapsibleItem
          icon={Database}
          label={<SkeletonRow />}
          triggerComponent={SidebarMenuButton}
          defaultOpen={database.schemas.length > 0}
          disabled
        >
          {database.schemas.map((schema, schemaIndex) => (
            <SidebarMenuSubItem key={`schema-${dbIndex}-${schemaIndex}`}>
              <TreeCollapsibleItem
                icon={FolderTree}
                label={<SkeletonRow />}
                triggerComponent={SidebarMenuSubButton}
                defaultOpen
                disabled
              >
                {Array.from({ length: schema.tables }).map((_, tableIndex) => (
                  <SidebarMenuSubItem key={`table-${dbIndex}-${schemaIndex}-${tableIndex}`}>
                    <SidebarMenuSubButton disabled>
                      <Table className="size-4" />
                      <SkeletonRow />
                    </SidebarMenuSubButton>
                  </SidebarMenuSubItem>
                ))}
              </TreeCollapsibleItem>
            </SidebarMenuSubItem>
          ))}
        </TreeCollapsibleItem>
      </SidebarMenuItem>
    ))}
  </>
);
