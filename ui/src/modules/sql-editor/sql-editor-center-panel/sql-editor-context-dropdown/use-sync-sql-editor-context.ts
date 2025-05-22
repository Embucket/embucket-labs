import { useEffect } from 'react';

import { useSqlEditorSettingsStore } from '../../sql-editor-settings-store';

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
  const { databaseName: selectedDatabase, schema: selectedSchema } = selectedContext;

  useEffect(() => {
    // No databases / schemas available - clear selection
    if (!databasesOptions.length || !schemasOptions.length) {
      setSelectedContext({ databaseName: '', schema: '' });
      return;
    }

    // Validate or set database
    const validDatabase =
      databasesOptions.find((opt) => opt.value === selectedDatabase)?.value ??
      databasesOptions[0].value;

    // No schemas available for the selected database - clear schema
    if (schemasOptions.length === 0) {
      setSelectedContext({ databaseName: validDatabase, schema: '' });
      return;
    }

    // Validate or set schema
    const validSchema =
      schemasOptions.find((opt) => opt.value === selectedSchema)?.value ?? schemasOptions[0].value;

    setSelectedContext({ databaseName: validDatabase, schema: validSchema });
  }, [selectedDatabase, selectedSchema, setSelectedContext, databasesOptions, schemasOptions]);
};
