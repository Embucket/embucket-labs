import { Database, Plus, Upload } from 'lucide-react';

export default function HomeActionButtons() {
  return (
    <div className="w-full px-4">
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        <button className="hover:bg-sidebar-secondary-accent bg-muted flex cursor-pointer items-center gap-3 rounded-md p-4 text-white transition-colors">
          <Plus className="text-muted-foreground h-5 w-5" />
          <span className="text-sm font-medium">Create a SQL Worksheet</span>
        </button>

        <button className="hover:bg-sidebar-secondary-accent bg-muted flex cursor-pointer items-center gap-3 rounded-md p-4 text-white transition-colors">
          <Upload className="text-muted-foreground h-5 w-5" />
          <span className="text-sm font-medium">Upload Local Files</span>
        </button>

        <button className="hover:bg-sidebar-secondary-accent bg-muted flex cursor-pointer items-center gap-3 rounded-md p-4 text-white transition-colors">
          <Database className="text-muted-foreground h-5 w-5" />
          <span className="text-sm font-medium">Create a New Database</span>
        </button>
      </div>
    </div>
  );
}
