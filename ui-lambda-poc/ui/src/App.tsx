import { useState, useEffect } from "react";

interface QueryHistory {
  id: number;
  sql: string;
  status: string;
  duration_ms: number;
  created_at?: string;
}

interface ApiResponse {
  success: boolean;
  data?: {
    rows: QueryHistory[];
  };
  error?: string;
}

function App() {
  const [queryHistory, setQueryHistory] = useState<QueryHistory[]>([]);
  const [loading, setLoading] = useState(true);
  const [inserting, setInserting] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date>(new Date());
  const [error, setError] = useState<string | null>(null);

  // Fetch Real Data from API
  const fetchData = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await fetch("/api/data");
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      
      const text = await response.text();
      if (text.trim().startsWith('<')) {
        throw new Error('Server returned HTML instead of JSON. Check API endpoint.');
      }
      
      const json: ApiResponse = JSON.parse(text);

      if (json.error) {
        throw new Error(json.error);
      }

      setQueryHistory(json.data?.rows || []);
    } catch (err: any) {
      console.error("Failed to fetch data:", err);
      setError(err.message || "Failed to fetch data");
    } finally {
      setLoading(false);
      setLastUpdated(new Date());
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  // Insert Real Data via API
  const insertRandomQueryHistory = async () => {
    setInserting(true);
    try {
      // Generate payload
      const tables = ['sales', 'users', 'products', 'events'];
      const table = tables[Math.floor(Math.random() * tables.length)];
      const payload = {
        sql: `SELECT * FROM ${table} WHERE id > ${Math.floor(Math.random() * 1000)}`,
        status: Math.random() > 0.2 ? "completed" : "error",
        duration_ms: Math.floor(Math.random() * 500) + 50,
      };

      const response = await fetch("/api/submit", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (response.ok) {
        await fetchData(); // Refresh list to show new DB row
      }
    } catch (error) {
      console.error("Failed to insert query history:", error);
    } finally {
      setInserting(false);
    }
  };

  return (
    <div className="min-h-screen bg-black py-12 px-4 flex flex-col items-center justify-center font-sans">
      <div className="w-full max-w-6xl mx-auto">
        
        {/* Titles */}
        <h1 className="text-5xl font-bold text-white text-center mb-3 tracking-tight">
          UI POC
        </h1>
        <h2 className="text-xl font-medium text-gray-500 text-center mb-2">
          Aurora PostgreSQL | Lambda Function URL
        </h2>
        <h3 className="font-medium text-gray-500 text-center mb-8">
          React | Express | Serverless-http | Pg
        </h3>

        <div className="max-w-lg mx-auto">
          {/* Query History Card */}
          <div className="bg-[radial-gradient(circle_at_center,rgb(39_39_42),rgb(24_24_27),rgb(9_9_11))] rounded-2xl shadow-2xl overflow-hidden flex flex-col h-full border border-zinc-700/50 backdrop-blur-sm">
            
            {/* Card Header */}
            <div className="px-6 py-5 border-b border-zinc-700/50">
              <div className="flex items-center justify-between flex-wrap gap-2">
                <div>
                  <h2 className="text-2xl font-semibold text-white tracking-tight">
                    Query History
                  </h2>
                  <p className="text-gray-500 text-sm mt-1.5">
                    {queryHistory.length}{" "}
                    {queryHistory.length === 1 ? "query" : "queries"}
                  </p>
                </div>
                <div className="text-xs text-gray-500 font-medium">
                  🕐 {new Date(lastUpdated).toLocaleString()}
                </div>
              </div>
            </div>

            {/* Error Banner */}
            {error && (
              <div className="bg-red-950/50 text-red-300 p-3 text-xs text-center border-b border-red-900/30 backdrop-blur-sm">
                Error: {error}
              </div>
            )}

            {/* List Content */}
            <div className="flex-1 p-6 overflow-y-auto max-h-96">
              {loading ? (
                <div className="flex items-center justify-center h-32">
                  <div className="animate-spin rounded-full h-8 w-8 border-2 border-zinc-700 border-t-white"></div>
                </div>
              ) : queryHistory.length === 0 ? (
                <div className="text-center text-gray-500 py-8">
                  No queries found in database.
                </div>
              ) : (
                <div className="space-y-3">
                  {queryHistory.map((item) => (
                    <div
                      key={item.id}
                      className="border border-zinc-700/50 rounded-xl p-4 bg-zinc-900/30 backdrop-blur-sm"
                    >
                      <div className="flex items-start justify-between mb-2">
                        <div className="flex-1">
                          <div className="font-mono text-sm text-gray-300 bg-black/50 p-3 rounded-lg border border-zinc-800/50 wrap-break-words backdrop-blur-sm">
                            {item.sql}
                          </div>
                        </div>
                      </div>
                      <div className="flex items-center justify-between mt-3">
                        <div className="flex items-center gap-3">
                          <span
                            className={`text-xs font-semibold px-2.5 py-1 rounded-md capitalize ${
                              item.status === "completed"
                                ? "bg-emerald-950/50 text-emerald-300 border border-emerald-900/30"
                                : "bg-red-950/50 text-red-300 border border-red-900/30"
                            }`}
                          >
                            {item.status}
                          </span>
                          <span className="text-xs text-gray-500">
                            ⚡ {item.duration_ms}ms
                          </span>
                          {item.created_at && (
                            <span className="text-xs text-gray-500">
                              🕐 {new Date(item.created_at).toLocaleTimeString()}
                            </span>
                          )}
                        </div>
                        <div className="text-xs text-gray-600">
                          ID: {item.id}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Footer / Insert Button */}
            <div className="border-t border-zinc-700/50 p-5">
              <button
                onClick={insertRandomQueryHistory}
                disabled={inserting || !!error}
                className="w-full bg-zinc-900 hover:bg-zinc-800/80 disabled:bg-zinc-900 disabled:opacity-60 disabled:cursor-not-allowed text-white font-semibold py-3 px-4 rounded-xl border border-zinc-700/50 hover:enabled:border-zinc-600/50 disabled:border disabled:border-zinc-700/40 transition-all duration-200 flex items-center justify-center gap-2 cursor-pointer"
              >
                {inserting ? (
                  <>
                    <div className="animate-spin rounded-full h-4 w-4 border-2 border-zinc-400 border-t-transparent"></div>
                    <span>Inserting...</span>
                  </>
                ) : (
                  <span>Insert Random Query</span>
                )}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;