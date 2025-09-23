import { useState } from 'react';

import { Check, Copy } from 'lucide-react';

import { Button } from '@/components/ui/button';
import type { QueryRecord } from '@/orval/models';

interface EditorRightPanelQueriesProps {
  query: QueryRecord;
}

export const EditorRightPanelQueryCopyButton = ({ query }: EditorRightPanelQueriesProps) => {
  const [hasCopied, setHasCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(query.query).then(() => {
      setHasCopied(true);
      setTimeout(() => setHasCopied(false), 1500);
    });
  };

  return (
    <Button
      variant="outline"
      className="hover:bg-hover! size-7! bg-transparent!"
      onClick={handleCopy}
    >
      {hasCopied ? <Check /> : <Copy />}
      <span className="sr-only">Copy query</span>
    </Button>
  );
};
