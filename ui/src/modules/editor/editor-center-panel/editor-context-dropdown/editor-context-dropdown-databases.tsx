import { ScrollArea } from '@/components/ui/scroll-area';
import { SidebarMenu, SidebarMenuButton } from '@/components/ui/sidebar';

// TODO: DRY
interface Option {
  value: string;
  label: string;
}

interface EditorContextDropdownDatabasesProps {
  databases: Option[];
  selectedDatabase: string | null;
  onSelectDatabase: (value: string) => void;
}

export const EditorContextDropdownDatabases = ({
  databases,
  selectedDatabase,
  onSelectDatabase,
}: EditorContextDropdownDatabasesProps) => {
  return (
    <ScrollArea className="max-h-60 border-r pr-2">
      <SidebarMenu>
        {databases.map((db) => (
          <SidebarMenuButton
            className="hover:bg-hover data-[active=true]:bg-hover!"
            key={db.value}
            onClick={() => onSelectDatabase(db.value)}
            isActive={selectedDatabase === db.value}
          >
            {db.label}
          </SidebarMenuButton>
        ))}
      </SidebarMenu>
    </ScrollArea>
  );
};
