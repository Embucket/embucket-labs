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
import useLocalStorage from 'use-local-storage';

interface EditorPanelsStateType {
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

  leftRef: RefObject<ImperativePanelHandle | null>;
  isLeftPanelExpanded: boolean;
  toggleLeftPanel: () => void;
  setLeftPanelExpanded: (expanded: boolean) => void;

  leftBottomRef: RefObject<ImperativePanelHandle | null>;
  isLeftBottomPanelExpanded: boolean;
  setLeftBottomPanelExpanded: (expanded: boolean) => void;

  rightRef: RefObject<ImperativePanelHandle | null>;
  isRightPanelExpanded: boolean;
  toggleRightPanel: () => void;
  setRightPanelExpanded: (expanded: boolean) => void;

  isDragging: boolean;
  setIsResizing: (resizing: boolean) => void;

  isAnyPanelCollapsing: boolean;
  setIsAnyPanelCollapsing: (collapsing: boolean) => void;
}

const EditorPanelsState = createContext<EditorPanelsStateType | undefined>(undefined);

interface EditorPanelsStateProviderProps {
  children: ReactNode;
}

export const EditorPanelsStateProvider = ({ children }: EditorPanelsStateProviderProps) => {
  const groupRef = useRef<ImperativePanelGroupHandle>(null);

  const topRef = useRef<ImperativePanelHandle>(null);
  const [isTopPanelExpanded, setTopPanelExpanded] = useLocalStorage('isTopPanelExpanded', false);

  const [isBottomPanelExpanded, setBottomPanelExpanded] = useLocalStorage(
    'isBottomPanelExpanded',
    false,
  );
  const bottomRef = useRef<ImperativePanelHandle>(null);

  const leftRef = useRef<ImperativePanelHandle>(null);
  const [isLeftPanelExpanded, setLeftPanelExpanded] = useLocalStorage('isLeftPanelExpanded', true);

  const leftBottomRef = useRef<ImperativePanelHandle>(null);
  const [isLeftBottomPanelExpanded, setLeftBottomPanelExpanded] = useLocalStorage(
    'isLeftBottomPanelExpanded',
    false,
  );

  const rightRef = useRef<ImperativePanelHandle>(null);
  const [isRightPanelExpanded, setRightPanelExpanded] = useLocalStorage(
    'isRightPanelExpanded',
    false,
  );

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
    if (!leftRef.current) return;

    if (isLeftPanelExpanded) {
      leftRef.current.collapse();
    } else {
      leftRef.current.expand();
    }
  }, [leftRef, isLeftPanelExpanded]);

  const toggleRightPanel = useCallback(() => {
    if (!rightRef.current) return;

    if (isRightPanelExpanded) {
      rightRef.current.collapse();
    } else {
      rightRef.current.expand();
    }
  }, [rightRef, isRightPanelExpanded]);

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
      leftRef,
      rightRef,
      setLeftPanelExpanded,
      setRightPanelExpanded,
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
      leftRef,
      rightRef,
      setLeftPanelExpanded,
      setRightPanelExpanded,
    ],
  );

  return <EditorPanelsState.Provider value={value}>{children}</EditorPanelsState.Provider>;
};

// eslint-disable-next-line react-refresh/only-export-components
export const useEditorPanelsState = () => {
  const context = useContext(EditorPanelsState);
  if (context === undefined) {
    throw new Error('useEditorPanelsState must be used within a EditorPanelsStateProvider');
  }
  return context;
};
