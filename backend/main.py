from __future__ import annotations
from email.utils import quote
import re
from pathlib import Path
import operator
from typing import TypedDict, List,Literal, Annotated, cast
from pathlib import Path
import os
import requests
from datetime import date
from google import genai
from google.genai import types
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field
import re # Add this import at the top
from pathlib import Path
from langgraph.graph import StateGraph, START, END
from langgraph.types import Send
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_groq import ChatGroq
from langchain_community.tools.tavily_search import TavilySearchResults
# Ensure you are importing from 'pydantic' and not 'pydantic.v1'
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from typing import List, Literal, Optional, Annotated
from langchain_core.rate_limiters import InMemoryRateLimiter


# ---------------- Load env ----------------
load_dotenv()


# ---------------- Models ----------------


import operator
from typing import List, Literal, Annotated, Optional
from typing_extensions import TypedDict
from pydantic import BaseModel, Field

# --- UPDATED TASK MODEL ---
class Task(BaseModel):
    id: int
    title: str
    goal: str
    
    @field_validator('bullets', mode='before')
    @classmethod
    def fix_bullets(cls, v):
        # Fixes: "minimum 2 items required, but found 1"
        if isinstance(v, list):
            if len(v) < 2: v.append("Detailed analysis of latest model updates.")
            return v[:5]
        return ["Analysis Point 1", "Analysis Point 2"]

    bullets: List[str] = Field(..., min_length=2, max_length=5)
    target_words: int
    section_type: Literal["intro", "core", "examples", "checklist", "common_mistakes", "conclusion"]
    requires_research: bool = False

class Plan(BaseModel):
    model_config = ConfigDict(extra='ignore') # Fixes: "extra field research_evidence"
    blog_title: str
    audience: str
    tone: str = "practical"
    blog_kind: Literal['explainer', 'tutorial', 'news_roundup', 'comparison', 'system_design']
    tasks: List[Task]
# --- RESEARCH MODELS ---

class RouterDecision(BaseModel):
    # Use field_validator for Pydantic V2 (current)
    @field_validator('needs_research', mode='before')
    @classmethod
    def coerce_to_bool(cls, v):
        if isinstance(v, str):
            return v.lower() in ('true', 'yes', '1', 't', 'y')
        return bool(v)

    needs_research: bool
    mode: Literal['closed_book', 'hybrid', 'open_book']
    reason: str
    queries: List[str]

class EvidenceItem(BaseModel):
    title: str
    url: str
    snippet: Optional[str] = None

class EvidencePack(BaseModel):
    model_config = ConfigDict(extra='ignore')
    evidence: List[EvidenceItem]

    @model_validator(mode='before')
    @classmethod
    def fix_json_structure(cls, data):
        # This fixes the common LLM error of wrapping list items in extra braces
        if isinstance(data, dict) and "evidence" in data:
            return data
        if isinstance(data, list):
            return {"evidence": data}
        return data



# --- GRAPH STATE ---
class State(TypedDict):
    topic: str
    
    # routing / research fields from image 3
    mode: str 
    needs_research: bool
    queries: List[str]
    evidence: List[EvidenceItem]
    plan: Optional[Plan]
    
    # workers
    # Using tuple[int, str] allows us to track (task_id, section_markdown)
    sections: Annotated[List[tuple[int, str]], operator.add] 
    final: str


class WorkerState(TypedDict):
    task: Task
    topic: str
    plan: Plan
    evidence: List[EvidenceItem]
   

# ---------------- LLM ----------------
rate_limiter = InMemoryRateLimiter(
    requests_per_second=0.33, 
    check_every_n_seconds=0.1, 
    max_bucket_size=2
)

# 2. Use a higher-limit model if possible, or stick to 8b with retries
llm = ChatGroq(
    model="openai/gpt-oss-120b",
    temperature=0.7,
    rate_limiter=rate_limiter, # SLOWS DOWN WORKERS
    max_retries=10             # AUTOMATICALLY WAITS AND RETRIES ON 429s
)


# ---------------- Nodes ----------------
ORCHESTRATOR_SYSTEM = """
<role>
You are a Senior Content Architect specializing in structured technical writing. 
Your goal is to transform research evidence into a validated Blog 'Plan' object.
</role>

<context>
- You will receive a 'topic' and a list of 'research_evidence' (titles and URLs).
- Use this evidence to ground the blog in facts.
- Distill the research into the 'research_summary' field.
</context>

<constraints>
1. SCHEMA FIDELITY: You MUST return ONLY a JSON object that strictly follows the 'Plan' schema.
2. NO EXTRA FIELDS: Never include fields like 'research_evidence' or 'name' in the top level.
3. TASK STRUCTURE: 
   - Every task must have exactly 3-5 bullets.
   - You must include exactly one 'section_type' as 'common_mistakes'.
   - Sections must follow a logical flow: intro -> core/checklist/examples -> conclusion.
4. RESEARCH FLAGS: Set 'requires_research' to true for tasks that need to cite the provided evidence.
</constraints>

<blog_kinds>
- explainer: High-level concepts.
- tutorial: Step-by-step guides.
- news_roundup: Recent developments/trends.
- comparison: Analyzing multiple tools/models.
- system_design: Deep architectural analysis.
</blog_kinds>
CRITICAL RULES:
1. Every task MUST have AT LEAST 2 bullet points.
2. The 'tone' field is MANDATORY. Do not leave it out.
3. For 'section_type', you can ONLY use: intro, core, examples, checklist, common_mistakes, conclusion.
   - If a section is a comparison, use 'core'.
"""
def orchestrator(state: State) -> dict:
    structured_llm = llm.with_structured_output(Plan).with_retry(stop_after_attempt=3)
    
    # Convert evidence list to a readable string for the prompt
    evidence_text = "\n".join([f"- {e.title}: {e.url}" for e in state.get("evidence", [])])
    
    plan = structured_llm.invoke(
        [
            SystemMessage(content=ORCHESTRATOR_SYSTEM),
            HumanMessage(content=(
                f"Topic: {state['topic']}\n"
                f"Audience: {state.get('audience', 'General Tech Readers')}\n"
                f"Research Evidence:\n{evidence_text}"
            )),
        ]
    )
    return {"plan": plan}
def fanout(state: State):
    # Trigger a parallel 'worker' for every task defined in the plan
    plan = state["plan"]
    if plan is None:
        raise ValueError("The planner did not return a plan")
    return [
        Send(
            "worker",
            {
                "task": task,
                "topic": state["topic"],
                "plan": state["plan"],
                "evidence": state.get("evidence", []) # Pass the research data
            },
        )
        for task in plan.tasks
    ]

def worker(state: WorkerState) -> dict:
    task = state["task"]
    topic = state["topic"]
    plan = state["plan"]
    evidence = state.get("evidence", [])

    # Format evidence into a string for the LLM to read
    def _evidence_line(e):
        if isinstance(e, dict):
            return f"- {e.get('title', 'Untitled')}: {e.get('url', '')}"
        return f"- {getattr(e, 'title', 'Untitled')}: {getattr(e, 'url', '')}"

    evidence_str = (
        "\n".join(_evidence_line(e) for e in evidence)
        if task.requires_research and evidence
        else "No specific research required for this section."
    )

    # Single LLM call per section (this node previously called the LLM twice
    # with the same prompt, doubling cost and latency)
    response = llm.invoke(
        [
            SystemMessage(
                content=(
                    "You are a professional technical blogger. "
                    "When writing your section, if research evidence is provided, "
                    "incorporate the facts naturally and mention the source title "
                    "where appropriate. Do not use Markdown footnotes, just mention them in-text."
                )
            ),
            HumanMessage(
                content=(
                    f"Blog Title: {plan.blog_title}\n"
                    f"Overall Topic: {topic}\n"
                    f"Audience: {plan.audience}\n"
                    f"Tone: {plan.tone}\n\n"
                    f"--- SECTION SPECIFICATIONS ---\n"
                    f"Section Title: {task.title}\n"
                    f"Section Type: {task.section_type}\n"
                    f"Goal: {task.goal}\n"
                    f"Key Subpoints:\n" + "\n".join([f"- {b}" for b in task.bullets]) + "\n"
                    f"Target Length: {task.target_words} words\n\n"
                    f"--- RESEARCH EVIDENCE ---\n"
                    f"{evidence_str}\n\n"
                    "Return ONLY the section body in Markdown. Do not repeat the section title."
                )
            ),
        ]
    )
    section_content = response.content
    section_md = section_content if isinstance(section_content, str) else str(section_content)
    section_md = section_md.strip()

    # Return as a list containing a tuple: [(id, content)]
    # This matches the Annotated[List[tuple[int, str]], operator.add] in your State
    return {"sections": [(task.id, section_md)]}



def reducer(state: State) -> dict:
    plan = state["plan"]
    if plan is None:
        raise ValueError("The planner did not return a plan")
    title = plan.blog_title
    
    # 1. Sort and join the body sections (as before)
    sorted_tuples = sorted(state["sections"], key=lambda x: x[0])
    body = "\n\n".join([content for _, content in sorted_tuples]).strip()

    # 2. GENERATE REFERENCES SECTION
    # We pull the evidence items that were stored during the research phase
    references_md = ""
    if state.get("evidence"):
        references_md = "\n\n---\n### Sources & References\n"
        # Use a set to ensure we don't list the same URL twice
        seen_urls = set()
        for item in state["evidence"]:
            url = item.get("url") if isinstance(item, dict) else item.url
            title = item.get("title") if isinstance(item, dict) else item.title
            if url and url not in seen_urls:
                references_md += f"- [{title or 'Source'}]({url})\n"
                seen_urls.add(url)

    # 3. Assemble Final Document
    final_md = (
        f"# {title}\n"
        f"**Target Audience:** {plan.audience}\n"
        f"**Tone:** {plan.tone}\n\n"
        f"{body}"
        f"{references_md}" # <--- Appending the sources here
    )

    return {"final": final_md}

ROUTER_SYSTEM = """You are a routing module for a technical blog planner.
Decide whether web research is needed BEFORE planning.

Modes:
- closed_book (needs_research=false):
  Evergreen topics where correctness does not depend on recent facts (concepts, fundamentals).
- hybrid (needs_research=true):
  Mostly evergreen but needs up-to-date examples/tools/models to be useful.
- open_book (needs_research=true):
  Mostly volatile: weekly roundups, "this week", "latest", rankings, pricing, policy/regulation.

If needs_research=true:
- Output 3-10 high-signal queries.
- Queries should be scoped and specific (avoid generic queries like just "AI" or "LLM").
- If user asked for "last week/this week/latest", reflect that constraint IN THE QUERIES.
CRITICAL DATA TYPE RULE:
For the 'needs_research' field, use ONLY the JSON boolean values: true or false.
DO NOT wrap them in quotes like "true" or "false".
"""
def router_node(state: State) -> dict:
    topic = state["topic"]
    
    router_llm = ChatGroq(model="openai/gpt-oss-20b", temperature=0)
    decider = router_llm.with_structured_output(RouterDecision)
    
    # 2. Invoke the model to decide the path
    decision = cast(RouterDecision, decider.invoke([
        SystemMessage(content=ROUTER_SYSTEM),
        HumanMessage(content=f"Topic: {topic}")
    ]))
    
    # 3. Return the decision to update the graph state
    return {
        "mode": decision.mode,
        "needs_research": decision.needs_research,
        "queries": decision.queries
    }
def route_next(state: State) -> str:
    """
    Checks the state to decide whether to go to the research node 
    or skip straight to planning.
    """
    if state["needs_research"]:
        return "research"
    else:
        return "orchestrator"
    
RESEARCH_SYSTEM = """You are a research synthesizer for technical writing.
Given raw web search results, produce a deduplicated list of EvidenceItem objects.

Rules:
- Only include items with a non-empty url.
- Prefer relevant + authoritative sources (company blogs, docs, reputable outlets).
- If a published date is explicitly present in the result payload, keep it as YYYY-MM-DD.
- If missing or unclear, set published_at=null. Do NOT guess.
- Keep snippets short.
- Deduplicate by URL.
"""
tavily_tool = TavilySearchResults(max_results=2)


def tavily_search(query: str) -> list[dict]:
    response = requests.post(
        "https://api.tavily.com/search",
        json={
            "api_key": os.environ.get("TAVILY_API_KEY", ""),
            "query": query,
            "max_results": 2,
            "search_depth": "basic",
        },
        timeout=20,
    )
    response.raise_for_status()
    results = response.json().get("results", [])
    return results if isinstance(results, list) else []


def research_node(state: State) -> dict:
    queries = state.get("queries", []) or []
    raw_results = []

    # 1. Execute searches (limit to 5 queries max to stay under TPM)
    for query in queries[:5]:
        try:
            search_output = tavily_search(query)
        except Exception as e:
            print(f"Search failed for query '{query}': {e}")
            continue

        # Tavily can return a plain string (an error/status message) instead of
        # a list of dicts. Guard against it, otherwise research_node crashes
        # with: AttributeError: 'str' object has no attribute 'get'
        if isinstance(search_output, str):
            print(f"Search returned a message instead of results for '{query}': {search_output[:200]}")
            continue
        if isinstance(search_output, dict):
            search_output = search_output.get("results", [])
        if not isinstance(search_output, list):
            continue
        raw_results.extend(r for r in search_output if isinstance(r, dict))

    # 2. Token-Saving Logic: Truncate snippets to 600 characters
    cleaned_results = []
    for r in raw_results:
        if not isinstance(r, dict):
            continue
        cleaned_results.append({
            "title": r.get("title") or "No Title",
            "url": r.get("url"),
            "content": (r.get("content") or "")[:600]
        })

    # Keep Tavily's URLs independently of the LLM so references cannot be lost
    # when the synthesizer omits or changes a source.
    source_evidence = [
        EvidenceItem(title=r["title"], url=r["url"])
        for r in cleaned_results
        if r.get("url")
    ]

    # 3. Synthesize with Retry Logic to handle "Failed to call function" errors
    # Note: Using a low temperature (0.1) improves formatting reliability
    synthesizer = llm.with_structured_output(EvidencePack).with_retry(stop_after_attempt=3)
    
    try:
        evidence_pack = cast(EvidencePack, synthesizer.invoke([
            SystemMessage(content=RESEARCH_SYSTEM + "\nReturn ONLY valid JSON."),
            HumanMessage(content=f"Synthesize this research data: {cleaned_results}")
        ]))
        synthesized_by_url = {item.url: item for item in evidence_pack.evidence if item.url}
        evidence = [synthesized_by_url.get(item.url, item) for item in source_evidence]
        return {"evidence": evidence}
    except Exception as e:
        print(f"Synthesis failed after retries: {e}")
        # Fallback: return raw results as evidence items if synthesis fails
        return {"evidence": source_evidence}

# ---------------- Graph ----------------

# --- Build main graph ---
g = StateGraph(State)

# Standard nodes
g.add_node("router", router_node)
g.add_node("research", research_node)
g.add_node("orchestrator", orchestrator)
g.add_node("worker", worker)

# FIX: Replace reducer_subgraph with your standard text-only reducer function
g.add_node("reducer", reducer) 

# --- Define Edges ---
g.add_edge(START, "router")

# Routing logic
g.add_conditional_edges(
    "router", 
    route_next, 
    {"research": "research", "orchestrator": "orchestrator"}
)
g.add_edge("research", "orchestrator")

# Fan-out to parallel workers
g.add_conditional_edges("orchestrator", fanout, ["worker"])

# Parallel Fan-in: All workers finish and trigger the text-only reducer
g.add_edge("worker", "reducer")
g.add_edge("reducer", END)

# Compile the full application
app = g.compile()

# ---------------- Run ----------------
def initial_state(topic: str) -> State:
    """Initial graph state, shared by run() and the streaming endpoint."""
    return {
        "topic": topic,
        "mode": "",                # Set by router
        "needs_research": False,   # Set by router
        "queries": [],             # Set by router
        "evidence": [],            # Collected by research_node
        "plan": None,              # Created by orchestrator
        "sections": [],            # Collected from parallel workers
        "final": "",               # Final result from the reducer
    }


def run(topic: str, as_of: Optional[str] = None):
    """
    Initializes and executes the blogwriting agent workflow (Text-Only Version).
    
    Args:
        topic: The subject of the blog post.
        as_of: Optional ISO format date string. Defaults to today's date.
    """
    # 1. Handle the research 'as_of' date logic
    if as_of is None:
        as_of = date.today().isoformat()

    # 2. Invoke the compiled graph with the simplified text-only state
    return app.invoke(initial_state(topic))
 
