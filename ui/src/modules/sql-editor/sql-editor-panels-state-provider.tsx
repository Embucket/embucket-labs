import type { RefObject } from 'react';
import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react';

import type { ImperativePanelGroupHandle, ImperativePanelHandle } from 'react-resizable-panels';

interface SqlEditorPanelsStateType {
  groupRef: RefObject<ImperativePanelGroupHandle | null>;
  alignPanels: () => void;

  topRef: RefObject<ImperativePanelHandle | null>;
  isTopPanelExpanded: boolean;
  setTopPanelExpanded: (expanded: boolean) => void;
  toggleTopPanel: () => void;

  bottomRef: RefObject<ImperativePanelHandle | null>;
  isBottomPanelExpanded: boolean;
  setBottomPanelExpanded: (expanded: boolean) => void;
  toggleBottomPanel: () => void;
  toggleLeftBottomPanel: () => void;

  isLeftPanelExpanded: boolean;
  toggleLeftPanel: () => void;

  leftBottomRef: RefObject<ImperativePanelHandle | null>;
  isLeftBottomPanelExpanded: boolean;
  setLeftBottomPanelExpanded: (expanded: boolean) => void;

  isRightPanelExpanded: boolean;
  toggleRightPanel: () => void;

  isDragging: boolean;
  setIsResizing: (resizing: boolean) => void;

  isAnyPanelCollapsing: boolean;
  setIsAnyPanelCollapsing: (collapsing: boolean) => void;
}

const SqlEditorPanelsState = createContext<SqlEditorPanelsStateType | undefined>(undefined);

interface SqlEditorPanelsStateProviderProps {
  children: ReactNode;
}

export const SqlEditorPanelsStateProvider = ({ children }: SqlEditorPanelsStateProviderProps) => {
  const groupRef = useRef<ImperativePanelGroupHandle>(null);

  const topRef = useRef<ImperativePanelHandle>(null);
  const [isTopPanelExpanded, setTopPanelExpanded] = useState(false);

  const [isBottomPanelExpanded, setBottomPanelExpanded] = useState(false);
  const bottomRef = useRef<ImperativePanelHandle>(null);

  const [isLeftPanelExpanded, setLeftPanelExpanded] = useState(true);

  const leftBottomRef = useRef<ImperativePanelHandle>(null);
  const [isLeftBottomPanelExpanded, setLeftBottomPanelExpanded] = useState(false);

  const [isRightPanelExpanded, setRightPanelExpanded] = useState(false);

  const [isDragging, setIsResizing] = useState(false);
  const [isAnyPanelCollapsing, setIsAnyPanelCollapsing] = useState(false);

  const toggleTopPanel = useCallback(() => {
    if (!topRef.current) return;

    if (isTopPanelExpanded) {
      topRef.current.collapse();
    } else {
      topRef.current.expand();
    }
  }, [topRef, isTopPanelExpanded]);

  const toggleBottomPanel = useCallback(() => {
    if (!bottomRef.current) return;

    if (isBottomPanelExpanded) {
      bottomRef.current.collapse();
    } else {
      bottomRef.current.expand();
    }
  }, [bottomRef, isBottomPanelExpanded]);

  const toggleLeftBottomPanel = useCallback(() => {
    if (!leftBottomRef.current) return;

    if (isLeftBottomPanelExpanded) {
      leftBottomRef.current.collapse();
    } else {
      leftBottomRef.current.expand();
    }
  }, [leftBottomRef, isLeftBottomPanelExpanded]);

  const toggleLeftPanel = useCallback(() => {
    setLeftPanelExpanded((prev) => !prev);
  }, [setLeftPanelExpanded]);

  const toggleRightPanel = useCallback(() => {
    setRightPanelExpanded((prev) => !prev);
  }, []);

  const alignPanels = useCallback(() => {
    if (!bottomRef.current) return;
    groupRef.current?.setLayout([50, 50]);
  }, [bottomRef, groupRef]);

  const value = useMemo(
    () => ({
      isAnyPanelCollapsing,
      setIsAnyPanelCollapsing,
      isDragging,
      setIsResizing,
      toggleRightPanel,
      toggleTopPanel,
      toggleBottomPanel,
      toggleLeftPanel,
      alignPanels,
      leftBottomRef,
      isLeftBottomPanelExpanded,
      setLeftBottomPanelExpanded,
      isTopPanelExpanded,
      isBottomPanelExpanded,
      isLeftPanelExpanded,
      isRightPanelExpanded,
      topRef,
      bottomRef,
      groupRef,
      setTopPanelExpanded,
      setBottomPanelExpanded,
      toggleLeftBottomPanel,
    }),
    [
      isAnyPanelCollapsing,
      setIsAnyPanelCollapsing,
      isDragging,
      setIsResizing,
      toggleRightPanel,
      toggleTopPanel,
      leftBottomRef,
      isLeftBottomPanelExpanded,
      setLeftBottomPanelExpanded,
      toggleBottomPanel,
      toggleLeftPanel,
      alignPanels,
      isTopPanelExpanded,
      isBottomPanelExpanded,
      isLeftPanelExpanded,
      isRightPanelExpanded,
      topRef,
      bottomRef,
      groupRef,
      setTopPanelExpanded,
      setBottomPanelExpanded,
      toggleLeftBottomPanel,
    ],
  );

  return <SqlEditorPanelsState.Provider value={value}>{children}</SqlEditorPanelsState.Provider>;
};

// eslint-disable-next-line react-refresh/only-export-components
export const useSqlEditorPanelsState = () => {
  const context = useContext(SqlEditorPanelsState);
  if (context === undefined) {
    throw new Error('useSqlEditorPanelsState must be used within a SqlEditorPanelsStateProvider');
  }
  return context;
};
