import { useEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";

const API_URL = import.meta.env.VITE_API_URL || (import.meta.env.DEV ? "http://localhost:8000" : "/api");

// Static pipeline stages. The writing steps are inserted dynamically
// once the orchestrator's plan arrives (one step per planned section).
const BASE_STAGES = [
  { key: "router", label: "🧭 Routing topic" },
  { key: "research", label: "🔎 Gathering research" },
  { key: "orchestrator", label: "🧠 Planning article" },
  { key: "reducer", label: "📦 Finalizing" },
];

export default function App() {
  const [topic, setTopic] = useState("");
  const [loading, setLoading] = useState(false);
  const [selectedBlog, setSelectedBlog] = useState("");
  const [files, setFiles] = useState([]);
  const [history, setHistory] = useState([]);
  const [stages, setStages] = useState([]); // { key, label, status: pending|running|done|skipped|error }
  const abortRef = useRef(null);

  const loadFiles = async () => {
    try {
      const res = await fetch(`${API_URL}/history`);
      const data = await res.json();
      setFiles(data.blogs || []);
    } catch {
      /* backend offline */
    }
  };

  useEffect(() => {
    loadFiles();
  }, []);

  const patchStage = (key, status) =>
    setStages((prev) => prev.map((s) => (s.key === key ? { ...s, status } : s)));

  const generate = async () => {
    if (!topic || loading) return;

    setLoading(true);
    setSelectedBlog("");
    setStages(BASE_STAGES.map((s) => ({ ...s, status: "pending" })));

    const controller = new AbortController();
    abortRef.current = controller;

    try {
      const res = await fetch(`${API_URL}/generate/stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ topic }),
        signal: controller.signal,
      });
      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        // SSE messages are separated by a blank line
        const parts = buffer.split("\n\n");
        buffer = parts.pop() || "";

        for (const part of parts) {
          const line = part.split("\n").find((l) => l.startsWith("data: "));
          if (!line) continue;
          let evt;
          try {
            evt = JSON.parse(line.slice(6));
          } catch {
            continue;
          }
          handleEvent(evt);
        }
      }
    } catch (err) {
      if (err.name !== "AbortError") {
        setStages((prev) =>
          prev.map((s) => (s.status === "running" ? { ...s, status: "error" } : s))
        );
        alert("Generation failed");
      }
    }

    setLoading(false);
    abortRef.current = null;
  };

  const handleEvent = (evt) => {
    switch (evt.type) {
      case "stage": {
        if (evt.status === "running") {
          // Mark every stage up to and including this one as running/done in order
          setStages((prev) => {
            const idx = prev.findIndex((s) => s.key === evt.stage);
            if (idx === -1) return prev;
            return prev.map((s, i) =>
              i < idx ? { ...s, status: s.status === "pending" ? "done" : s.status }
              : i === idx ? { ...s, status: "running" }
              : s
            );
          });
        } else if (evt.stage === "research" && evt.status === "done") {
          patchStage("research", "done");
          patchStage("orchestrator", "running");
        } else {
          patchStage(evt.stage, "done");
        }
        break;
      }
      case "queries": {
        if (evt.queries?.length) {
          setStages((prev) =>
            prev.map((s) =>
              s.key === "research" ? { ...s, detail: `${evt.queries.length} searches queued` } : s
            )
          );
        }
        break;
      }
      case "plan": {
        // Insert one writing step per planned section, before the final stage.
        // They start as "pending" and flip to running/done one-by-one.
        const writing = evt.sections.map((title, i) => ({
          key: `section-${i}`,
          label: `✍ Writing: ${title}`,
          status: "pending",
        }));
        setStages((prev) => {
          const final = prev.find((s) => s.key === "reducer");
          return [...prev.filter((s) => s.key !== "reducer"), ...writing, final].filter(Boolean);
        });
        break;
      }
      case "section": {
        // Each section completion arrives as its own event → shown one-by-one
        const key = `section-${evt.position}`;
        setStages((prev) =>
          prev.map((s) =>
            s.key === key
              ? { ...s, status: "done" }
              : s.key === `section-${evt.position + 1}` && s.status === "pending"
              ? { ...s, status: "running" }
              : s
          )
        );
        break;
      }
      case "done": {
        setStages((prev) =>
          prev.map((s) =>
            s.status === "pending" || s.status === "running" ? { ...s, status: "done" } : s
          )
        );
        setSelectedBlog(evt.markdown || "");
        setHistory((h) => [...h, { role: "user", content: topic }]);
        loadFiles();
        break;
      }
      case "error": {
        setStages((prev) =>
          prev.map((s) => (s.status === "running" || s.status === "pending" ? { ...s, status: "error" } : s))
        );
        alert(`Generation failed: ${evt.message}`);
        break;
      }
    }
  };

  const stop = () => {
    abortRef.current?.abort();
  };

  const statusIcon = (status) =>
    status === "done" ? " ✅"
    : status === "running" ? " ⏳"
    : status === "error" ? " ❌"
    : "";

  return (
    <div style={{ display: "flex", height: "100vh", fontFamily: "sans-serif" }}>
      {/* SIDEBAR */}
      <div style={{ width: 280, borderRight: "1px solid #ddd", padding: 20, overflow: "auto" }}>
        <h3>📚 Generated Blogs</h3>
        {files.length === 0 && <p>No blogs yet.</p>}
        {files.map((blog) => (
          <div key={blog.id} style={{ marginBottom: 8 }}>
            <button
              style={{ width: "100%", textAlign: "left" }}
              onClick={async () => {
                const res = await fetch(`${API_URL}/file/${blog.id}`);
                setSelectedBlog(await res.text());
              }}
            >
              📄 {blog.title}
            </button>
          </div>
        ))}
      </div>

      {/* MAIN AREA */}
      <div style={{ flex: 1, display: "flex" }}>
        {/* INPUT */}
        <div style={{ width: "35%", padding: 20, borderRight: "1px solid #ddd", overflow: "auto" }}>
          <h2>Create New Content</h2>

          <textarea
            value={topic}
            onChange={(e) => setTopic(e.target.value)}
            placeholder="What should the blog be about?"
            style={{ width: "100%", height: 150 }}
          />

          <div style={{ display: "flex", gap: 8, marginTop: 10 }}>
            <button onClick={generate} disabled={loading || !topic} style={{ flex: 1 }}>
              {loading ? "🏗️ Working..." : "🚀 Build Blog"}
            </button>
            {loading && (
              <button onClick={stop} style={{ flex: 1 }}>
                ✋ Stop
              </button>
            )}
          </div>

          {/* Live pipeline — agents appear one-by-one as they actually run */}
          {stages.length > 0 && (
            <div style={{ marginTop: 15 }}>
              <h4>Agent Pipeline</h4>
              {stages.map((s) => (
                <p key={s.key} style={{ margin: 2, opacity: s.status === "pending" ? 0.4 : 1 }}>
                  {s.label}
                  {s.detail ? ` (${s.detail})` : ""}
                  {statusIcon(s.status)}
                </p>
              ))}
            </div>
          )}

          <hr />
          <h4>Recent Activity</h4>
          {history.slice(-4).reverse().map((h, i) => (
            <div key={i}>
              <b>{h.role}:</b> {h.content}
            </div>
          ))}
        </div>

        {/* DISPLAY */}
        <div style={{ flex: 1, padding: 20, overflow: "auto" }}>
          <h2>📖 Content Viewer</h2>
          {selectedBlog ? (
            <>
              <ReactMarkdown>{selectedBlog}</ReactMarkdown>
              <button
                onClick={() => {
                  const blob = new Blob([selectedBlog], { type: "text/markdown" });
                  const url = URL.createObjectURL(blob);
                  const a = document.createElement("a");
                  a.href = url;
                  a.download = "generated_blog.md";
                  a.click();
                  URL.revokeObjectURL(url);
                }}
              >
                📥 Download Markdown
              </button>
            </>
          ) : (
            <p>The generated blog will appear here.</p>
          )}
        </div>
      </div>
    </div>
  );
}
