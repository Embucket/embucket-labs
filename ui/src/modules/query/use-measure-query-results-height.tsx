import { useEffect, useLayoutEffect, useRef, useState } from 'react';

import { useDebounce } from '@/hooks/use-debounce';

// TODO: No way we do this.
export function useMeasureQueryResultsHeight({ isReady }: { isReady: boolean }) {
  const detailsRef = useRef<HTMLDivElement>(null);
  const [detailsHeight, setDetailsHeight] = useState(0);
  const [windowWidth, setWindowWidth] = useState(window.innerWidth);
  const debouncedWindowWidth = useDebounce(windowWidth, 500);

  const measureHeight = () => {
    if (detailsRef.current) {
      // Force layout recalculation
      detailsRef.current.style.display = 'none';
      void detailsRef.current.offsetHeight; // Force reflow
      detailsRef.current.style.display = '';

      const height = detailsRef.current.getBoundingClientRect().height;
      setDetailsHeight(height);
    }
  };

  // Initial measurement and setup
  useLayoutEffect(() => {
    if (isReady) {
      measureHeight();
    }

    const handleResize = () => {
      setWindowWidth(window.innerWidth);
    };

    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
    };
  }, [isReady]);

  // Re-measure when content changes or window is resized
  useEffect(() => {
    if (isReady) {
      measureHeight();
    }
  }, [detailsRef.current?.innerHTML, isReady, debouncedWindowWidth]);

  return {
    detailsRef,
    tableStyle: {
      height: `calc(100vh - 65px - 48px - 2px - ${detailsHeight}px)`,
    },
  };
}
