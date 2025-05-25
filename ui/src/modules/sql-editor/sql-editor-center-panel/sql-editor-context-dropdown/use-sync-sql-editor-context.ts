import { useEffect } from 'react';

import { useSqlEditorSettingsStore } from '../../sql-editor-settings-store';

// TODO: DRY
interface SelectOption {
  value: string;
  label: string;
}

interface UseSyncSqlEditorContextProps {
  databasesOptions: SelectOption[];
  schemasOptions: SelectOption[];
}

export const useSyncSqlEditorContext = ({
  databasesOptions,
  schemasOptions,
}: UseSyncSqlEditorContextProps) => {
  const { selectedContext, setSelectedContext } = useSqlEditorSettingsStore();
  const { database: selectedDatabase, schema: selectedSchema } = selectedContext;

  useEffect(() => {
    // No databases / schemas available - clear selection
    if (!databasesOptions.length || !schemasOptions.length) {
      setSelectedContext({ database: '', schema: '' });
      return;
    }

    // Both databases and schemas are available - validate or set defaults
    const validDatabase =
      databasesOptions.find((opt) => opt.value === selectedDatabase)?.value ??
      databasesOptions[0].value;
    const validSchema =
      schemasOptions.find((opt) => opt.value === selectedSchema)?.value ?? schemasOptions[0].value;

    setSelectedContext({ database: validDatabase, schema: validSchema });
  }, [selectedDatabase, selectedSchema, setSelectedContext, databasesOptions, schemasOptions]);
};
