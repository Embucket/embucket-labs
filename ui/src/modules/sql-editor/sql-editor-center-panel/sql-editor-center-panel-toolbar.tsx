import { useEffect, useState } from 'react';

import type { SqlStatement } from '@tidbcloud/codemirror-extension-sql-parser';
import { useEditorCacheContext } from '@tidbcloud/tisqleditor-react';
import { Database, Share2, Wand2 } from 'lucide-react';
import { format } from 'sql-formatter';

import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { SidebarGroup } from '@/components/ui/sidebar';
// import type { Volume } from '@/orval/models';
import { useGetVolumes } from '@/orval/volumes';

interface RunSQLButtonProps {
  onRunQuery: (query: string) => void;
  disabled?: boolean;
}

function RunSQLButton({ onRunQuery, disabled }: RunSQLButtonProps) {
  const cacheCtx = useEditorCacheContext();

  function handleRunQuery() {
    const activeEditor = cacheCtx.getEditor('MySQLEditor');
    if (!activeEditor) return;
    let targetStatement: SqlStatement | undefined;
    const curStatements = activeEditor.getCurStatements();

    if (curStatements[0].content === '') {
      targetStatement = activeEditor.getNearbyStatement();
    } else {
      targetStatement = curStatements.at(-1);
    }
    if (!targetStatement?.content) return;

    onRunQuery(targetStatement.content);
  }

  return (
    <div className="w-fit">
      <Button size="sm" className="justify-start" disabled={disabled} onClick={handleRunQuery}>
        Run <span className="opacity-40">⌘ ⏎</span>
      </Button>
    </div>
  );
}

const BeautifyButton = () => {
  const cacheCtx = useEditorCacheContext();

  const handleBeautify = () => {
    const activeEditor = cacheCtx.getEditor('MySQLEditor');
    if (!activeEditor) return;

    const fullContent = activeEditor.editorView.state.doc.toString();
    const beautified = format(fullContent);
    // console.log(beautified, beautified.length);
    activeEditor.editorView.dispatch({
      changes: { from: 0, to: activeEditor.editorView.state.doc.length, insert: beautified },
      selection: { anchor: activeEditor.editorView.state.doc.length },
    });
  };

  return (
    <Button
      onClick={handleBeautify}
      size="icon"
      variant="ghost"
      className="text-muted-foreground size-8"
    >
      <Wand2 />
    </Button>
  );
};

// const DATA: Volume[] = [
//   {
//     type: 'memory',
//     name: 'myvolume',
//   },
// ];

const VolumeSelect = () => {
  const [selectedOption, setSelectedOption] = useState<string | undefined>();
  const { data: { items: volumes } = {}, isPending } = useGetVolumes();

  useEffect(() => {
    if (volumes?.length && !isPending && !selectedOption) {
      setSelectedOption(volumes[0].name);
    }
  }, [volumes, selectedOption, isPending]);

  return (
    <Select
      value={selectedOption}
      onValueChange={(value) => {
        setSelectedOption(value); // No error now, as the type matches
      }}
      disabled
    >
      <SelectTrigger className="hover:bg-sidebar-secondary-accent! h-8! border-none bg-transparent! outline-0">
        <div className="flex items-center gap-2">
          <Database className="size-4" />
          <SelectValue />
        </div>
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          {volumes?.map((volume) => (
            <SelectItem key={volume.name} value={volume.name}>
              {volume.name}
            </SelectItem>
          ))}
        </SelectGroup>
      </SelectContent>
    </Select>
  );
};

interface SqlEditorToolbarProps {
  onRunQuery: (query: string) => void;
}

export const SqlEditorCenterPanelToolbar = ({ onRunQuery }: SqlEditorToolbarProps) => {
  return (
    <div>
      <SidebarGroup className="flex justify-between border-b p-4">
        <div className="flex items-center gap-2">
          {/* <BeautifyButton /> */}
          <RunSQLButton onRunQuery={onRunQuery} />
          <VolumeSelect />
          <div className="ml-auto flex items-center gap-2">
            <Button disabled size="icon" variant="ghost" className="text-muted-foreground size-8">
              <Share2 />
            </Button>
            <BeautifyButton />
          </div>
        </div>
      </SidebarGroup>
    </div>
  );
};
