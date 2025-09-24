import type { RefObject } from 'react';
import { useEffect } from 'react';

interface UseEditorScrollToSelectedCellProps {
  selectedCellId?: string;
  scrollRootRef: RefObject<HTMLDivElement | null>;
}

export const useEditorScrollToSelectedCell = ({
  selectedCellId,
  scrollRootRef,
}: UseEditorScrollToSelectedCellProps) => {
  useEffect(() => {
    if (!selectedCellId) return;

    const root = scrollRootRef.current;
    if (!root) return;

    // Find the selected cell inside the scroll viewport and ensure it's visible
    const cell = root.querySelector<HTMLTableCellElement>(`[data-cell-id="${selectedCellId}"]`);
    if (cell) {
      cell.scrollIntoView({ block: 'center', inline: 'center' });
    }
  }, [selectedCellId, scrollRootRef]);
};
