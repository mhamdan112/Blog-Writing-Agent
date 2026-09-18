import json
import os

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from supabase import create_client

from main import Plan, State, initial_state, run
from main import app as blog_graph  # compiled LangGraph app

load_dotenv()

supabase_url = os.environ["SUPABASE_URL"]
supabase_key = os.environ["SUPABASE_KEY"]
supabase = create_client(supabase_url, supabase_key)

# ✅ CREATE APP FIRST
app = FastAPI()

# ✅ THEN middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ THEN routes

# Display order of graph nodes (research is skipped for closed-book topics)
STAGE_ORDER = ["router", "research", "orchestrator", "worker", "reducer"]


def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload)}\n\n"


@app.post("/generate/stream")
def generate_stream(req: dict):
    """Runs the blog graph and streams each agent's completion as it happens."""
    topic = req["topic"]

    def event_stream():
        state: State = initial_state(topic)
        id_positions: dict = {}
        section_titles: dict = {}
        sections_done = 0
        try:
            yield _sse({"type": "stage", "stage": "router", "status": "running"})

            # stream_mode="updates" yields one update per node, as soon as that
            # node finishes - so events reach the UI one-by-one, in real order.
            for update in blog_graph.stream(state, stream_mode="updates"):
                for node, payload in update.items():
                    if node.startswith("__") or node not in STAGE_ORDER:
                        continue
                    if payload:
                        state.update(payload)

                    if node == "research":
                        yield _sse({"type": "queries", "queries": state.get("queries") or []})
                    elif node == "router":
                        next_stage = "research" if state.get("needs_research") else "orchestrator"
                        yield _sse({"type": "stage", "stage": next_stage, "status": "running"})
                    elif node == "orchestrator" and state.get("plan"):
                        plan = state["plan"]
                        if not isinstance(plan, Plan):
                            continue
                        id_positions = {t.id: i for i, t in enumerate(plan.tasks)}
                        section_titles = {t.id: t.title for t in plan.tasks}
                        yield _sse({
                            "type": "plan",
                            "title": plan.blog_title,
                            "sections": [t.title for t in plan.tasks],
                        })
                    elif node == "worker" and payload:
                        # One update per section (sections are written in parallel,
                        # but the UI shows each one the moment it completes)
                        for sid, _md in payload.get("sections", []):
                            sections_done += 1
                            yield _sse({
                                "type": "section",
                                "title": section_titles.get(sid, f"Section {sid}"),
                                "position": id_positions.get(sid, sections_done - 1),
                                "completed": sections_done,
                            })

                    yield _sse({"type": "stage", "stage": node, "status": "done"})

            markdown = state.get("final", "")
            lines = markdown.splitlines() if markdown else []
            title = lines[0].removeprefix("# ").strip() if lines else "Untitled blog"
            saved = True
            try:
                supabase.table("blogs").insert({"title": title, "content": markdown}).execute()
            except Exception as e:
                saved = False
                print(f"Supabase insert failed: {e}")
            yield _sse({"type": "done", "markdown": markdown, "title": title, "saved": saved})
        except Exception as e:
            yield _sse({"type": "error", "message": str(e)})

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/generate")
def generate(req: dict):
    result = run(req["topic"])
    markdown = result["final"]
    title = markdown.splitlines()[0].removeprefix("# ").strip() or "Untitled blog"
    saved = supabase.table("blogs").insert({
        "title": title,
        "content": markdown,
    }).execute()
    return {"markdown": result["final"]}


@app.get("/history")
def history():
    response = supabase.table("blogs").select("id,title,created_at").order(
        "created_at", desc=True
    ).execute()
    return {"blogs": response.data}


@app.get("/file/{blog_id}")
def read_file(blog_id: str):
    response = supabase.table("blogs").select("content").eq("id", blog_id).single().execute()
    if not isinstance(response.data, dict) or "content" not in response.data:
        raise ValueError("Blog was not found")
    return response.data["content"]
