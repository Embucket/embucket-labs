import { useEffect, useLayoutEffect, useRef, useState } from 'react';

// TODO: No way it's a good thing to do.
export function useMeasureQueryResultsHeight() {
  const detailsRef = useRef<HTMLDivElement>(null);
  const [detailsHeight, setDetailsHeight] = useState(0);

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
    measureHeight();

    // Debounced resize handler
    let timeoutId: NodeJS.Timeout;
    const handleResize = () => {
      clearTimeout(timeoutId);
      timeoutId = setTimeout(measureHeight, 100);
    };

    window.addEventListener('resize', handleResize);
    window.addEventListener('load', measureHeight);

    return () => {
      window.removeEventListener('resize', handleResize);
      window.removeEventListener('load', measureHeight);
      clearTimeout(timeoutId);
    };
  }, []);

  // Re-measure when content changes
  useEffect(() => {
    measureHeight();
  }, [detailsRef.current?.innerHTML]);

  return {
    detailsRef,
    tableStyle: {
      height: `calc(100vh - 65px - 48px - 2px - ${detailsHeight}px)`,
    },
  };
}
