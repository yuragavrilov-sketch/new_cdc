import { useCallback, useEffect, useRef, useState } from "react";

interface UseApiOpts {
  /** Auto-poll interval in ms. 0/undefined = no polling. */
  intervalMs?: number;
  /** Re-fetch when any of these change. */
  deps?: unknown[];
  /** If false, do not fetch automatically — only via reload(). */
  enabled?: boolean;
}

export interface UseApiResult<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  reload: () => void;
  setData: (next: T | null) => void;
}

/**
 * Minimal fetch hook with loading/error/polling.
 * Replaces repeated useEffect(fetch().then().catch()) + setInterval patterns.
 */
export function useApi<T>(url: string | null, opts: UseApiOpts = {}): UseApiResult<T> {
  const { intervalMs, deps = [], enabled = true } = opts;
  const [data,    setData]    = useState<T | null>(null);
  const [loading, setLoading] = useState<boolean>(enabled);
  const [error,   setError]   = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const reload = useCallback(() => {
    if (!url || !enabled) return;
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    setLoading(true);
    fetch(url, { signal: ctrl.signal })
      .then(r => r.ok ? r.json() : Promise.reject(`HTTP ${r.status}`))
      .then((json: T) => {
        if (!ctrl.signal.aborted) {
          setData(json);
          setError(null);
          setLoading(false);
        }
      })
      .catch(e => {
        if (ctrl.signal.aborted) return;
        if (e?.name === "AbortError") return;
        setError(String(e));
        setLoading(false);
      });
  }, [url, enabled]);

  useEffect(() => {
    reload();
    return () => abortRef.current?.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [url, enabled, ...deps]);

  useEffect(() => {
    if (!intervalMs || !enabled || !url) return;
    const id = setInterval(reload, intervalMs);
    return () => clearInterval(id);
  }, [reload, intervalMs, enabled, url]);

  return { data, loading, error, reload, setData };
}
