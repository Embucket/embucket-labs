import type { NavigationTreeDatabase } from '@/orval/models';

export const NAVIGATION_TREES_MOCK: NavigationTreeDatabase[] = [
  {
    name: 'slatedb',
    schemas: [
      {
        name: 'history',
        tables: [],
        views: [
          {
            name: 'worksheets',
          },
          {
            name: 'queries',
          },
        ],
      },
      {
        name: 'information_schema',
        tables: [],
        views: [
          {
            name: 'schemata',
          },
          {
            name: 'columns',
          },
          {
            name: 'parameters',
          },
          {
            name: 'df_settings',
          },
          {
            name: 'navigation_tree',
          },
          {
            name: 'views',
          },
          {
            name: 'tables',
          },
          {
            name: 'routines',
          },
          {
            name: 'databases',
          },
        ],
      },
      {
        name: 'meta',
        tables: [],
        views: [
          {
            name: 'schemas',
          },
          {
            name: 'tables',
          },
          {
            name: 'databases',
          },
          {
            name: 'volumes',
          },
        ],
      },
    ],
  },
  {
    name: 'snowplow',
    schemas: [
      {
        name: '"test212"',
        tables: [],
        views: [],
      },
      {
        name: 'information_schema',
        tables: [],
        views: [
          {
            name: 'schemata',
          },
          {
            name: 'columns',
          },
          {
            name: 'parameters',
          },
          {
            name: 'df_settings',
          },
          {
            name: 'navigation_tree',
          },
          {
            name: 'views',
          },
          {
            name: 'tables',
          },
          {
            name: 'routines',
          },
          {
            name: 'databases',
          },
        ],
      },
      {
        name: 'public',
        tables: [
          {
            name: 'events_iceberg',
          },
        ],
        views: [],
      },
    ],
  },
];
