import { Hash } from 'lucide-react';

interface EditorLeftPanelTableColumnProps {
  name: string;
  type: string;
}

export function EditorLeftPanelTableColumn({ name, type }: EditorLeftPanelTableColumnProps) {
  return (
    <div className="flex items-center justify-between text-xs select-none">
      <div className="flex items-center overflow-hidden py-2">
        <Hash className="text-muted-foreground size-4 flex-shrink-0" />
        <p className="mx-2 truncate">{name}</p>
      </div>
      <span className="text-muted-foreground flex-shrink-0">{type}</span>
    </div>
  );
}
