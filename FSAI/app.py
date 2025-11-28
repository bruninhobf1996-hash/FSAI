#FSAI
import os, json, math, re, yaml
from typing import List, Dict, Any, Optional, Tuple
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from openai import OpenAI
import mysql.connector  # >>> substitui o psycopg por MySQL

load_dotenv()

# ---------------- Config ----------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
if not OPENAI_API_KEY:
    raise RuntimeError("Defina OPENAI_API_KEY no .env")

client = OpenAI(api_key=OPENAI_API_KEY)

GEN_MODEL   = os.getenv("GEN_MODEL", "gpt-4o-mini")
EMBED_MODEL = os.getenv("EMBED_MODEL", "text-embedding-3-small")

MYSQL_HOST = os.getenv("MYSQL_HOST", "45.33.0.225")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "")
if not MYSQL_DATABASE:
    raise RuntimeError("Defina MYSQL_DATABASE no .env")

MAX_ROWS          = int(os.getenv("MAX_ROWS", "200"))
TOPK_OBJECTS      = int(os.getenv("TOPK_OBJECTS", "5"))
TOPK_COLS_PER_TBL = int(os.getenv("TOPK_COLS_PER_TBL", "6"))
SCHEMA_PATH       = os.getenv("SCHEMA_PATH", "schema.yaml")

SQL_FORBIDDEN = re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|TRUNCATE|GRANT|REVOKE|CREATE)\b", re.I)

# ---------------- Modelos ----------------
class AskBody(BaseModel):
    user_id: str
    department: Optional[str] = None
    prompt: str
    lang: str = "pt-BR"

# ---------------- Util: Embeddings ----------------
def embed_texts(texts: List[str]) -> List[List[float]]:
    resp = client.embeddings.create(model=EMBED_MODEL, input=texts)
    return [d.embedding for d in resp.data]

def cosine(a: List[float], b: List[float]) -> float:
    dot = sum(x*y for x,y in zip(a,b))
    na = math.sqrt(sum(x*x for x in a)) + 1e-12
    nb = math.sqrt(sum(y*y for y in b)) + 1e-12
    return dot/(na*nb)

# ---------------- Catálogo RAG ----------------
with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    ALLOWLIST = yaml.safe_load(f)

OBJ_INDEX: List[Dict[str, Any]] = []
def build_index():
    items = []
    for ds in ALLOWLIST.get("datasets", []):
        ds_name = ds["name"]
        for t in ds.get("tables", []):
            t_name = t["name"]
            t_desc = t.get("description", "")
            items.append({
                "kind": "table",
                "ds": ds_name,
                "table": t_name,
                "col": None,
                "text": f"{ds_name}.{t_name}: {t_desc}".strip()
            })
            for c in t.get("columns", []):
                c_name = c["name"]
                c_desc = c.get("description", "")
                items.append({
                    "kind": "column",
                    "ds": ds_name,
                    "table": t_name,
                    "col": c_name,
                    "text": f"{ds_name}.{t_name}.{c_name}: {c_desc}".strip()
                })
    embs = embed_texts([it["text"] for it in items]) if items else []
    for it, e in zip(items, embs):
        it["emb"] = e
    return items

OBJ_INDEX = build_index()

def retrieve_schema_objects(prompt: str, topk_tables: int = TOPK_OBJECTS) -> Dict[str, Any]:
    """Seleciona tabelas/colunas mais relevantes ao prompt."""
    if not OBJ_INDEX:
        return {"tables": []}
    q_emb = embed_texts([prompt])[0]
    scored = [(cosine(q_emb, it["emb"]), it) for it in OBJ_INDEX]
    scored.sort(key=lambda x: x[0], reverse=True)

    table_map: Dict[Tuple[str,str], Dict[str, Any]] = {}
    for _, it in scored:
        key = (it["ds"], it["table"])
        if key not in table_map:
            table_map[key] = {"ds": it["ds"], "table": it["table"], "table_score": 0.0, "cols": []}
        if it["kind"] == "table":
            table_map[key]["table_score"] = max(table_map[key]["table_score"], 1.0)
        else:
            table_map[key]["cols"].append({"name": it["col"], "text": it["text"]})

    ranked_tables = sorted(table_map.values(), key=lambda x: (len(x["cols"]), x["table_score"]), reverse=True)
    ranked_tables = ranked_tables[:topk_tables]
    for t in ranked_tables:
        t["cols"] = t["cols"][:TOPK_COLS_PER_TBL]
    return {"tables": ranked_tables}

# ---------------- SQL (Geração e Execução) ----------------
def sanitize_sql(sql: str) -> str:
    if SQL_FORBIDDEN.search(sql):
        raise ValueError("Comando SQL proibido (DDL/DML).")
    if not re.match(r"^\s*SELECT\b", sql, re.I):
        raise ValueError("Apenas SELECT é permitido.")
    if re.search(r"\bLIMIT\s+\d+\b", sql, re.I) is None:
        sql = f"{sql.strip()} LIMIT {MAX_ROWS}"
    return sql

def llm_generate_sql(prompt: str, schema_hint: Dict[str, Any]) -> str:
    """Gera SQL restrito com base nos objetos encontrados no schema.yaml"""
    lines = []
    for t in schema_hint.get("tables", []):
        ds, tb = t["ds"], t["table"]
        col_list = ", ".join([c["name"] for c in t["cols"]]) if t["cols"] else "*"
        lines.append(f"{ds}.{tb}({col_list})")
    hint = "\n".join(lines) if lines else "(nenhum)"

    system = (
        "Você gera um SELECT seguro, SOMENTE usando os objetos listados abaixo, sem DDL/DML.\n"
        f"Objetos permitidos:\n{hint}\n"
        f"Restrições: use apenas tabelas/colunas listadas; aplique LIMIT {MAX_ROWS}; responda apenas com SQL puro."
    )

    out = client.chat.completions.create(
        model=GEN_MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": f"Pergunta do gestor: {prompt}"}
        ],
        temperature=0
    )
    sql = out.choices[0].message.content.strip()
    return sanitize_sql(sql)

def run_sql(sql: str) -> List[Dict[str, Any]]:
    """Executa consulta no MySQL DW"""
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    cur = conn.cursor()
    cur.execute(sql)
    cols = [desc[0] for desc in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows[:MAX_ROWS]

# ---------------- Contexto e Resposta ----------------
def build_context(prompt: str, schema_hint: Dict[str, Any], preview_rows: List[Dict[str, Any]]) -> str:
    schema_txt = []
    for t in schema_hint.get("tables", []):
        ds, tb = t["ds"], t["table"]
        cols = ", ".join([c["name"] for c in t["cols"]]) if t["cols"] else "*"
        schema_txt.append(f"- {ds}.{tb}({cols})")
    preview = json.dumps(preview_rows[:20], ensure_ascii=False, indent=2)

    return (
        "### Objetos do DW selecionados (RAG sobre catálogo)\n"
        + "\n".join(schema_txt)
        + "\n\n### Amostra de dados do DW (pré-agrupada pelo SQL gerado)\n"
        + preview
        + "\n\n### Observações\n- Use apenas as informações acima.\n- Não exiba SQL na resposta.\n"
    )

def answer_natural_language(prompt: str, context: str, lang: str = "pt-BR") -> str:
    system = (
        "Você é um analista corporativo. Elabore uma resposta clara e objetiva apenas com base no Contexto. "
        "Se algo não estiver no contexto, diga claramente que não há dados suficientes. "
        "Apresente números em formato legível, e use listas curtas quando fizer sentido."
    )
    user = f"Pergunta: {prompt}\n\nContexto:\n{context}\n\nResponda em {lang}."
    out = client.chat.completions.create(
        model=GEN_MODEL,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        temperature=0.3
    )
    return out.choices[0].message.content.strip()

# ---------------- API ----------------
app = FastAPI(title="RAG para DW (MySQL) — Linguagem Natural", version="1.0")

@app.post("/ask")
def ask(body: AskBody):
    schema_hint = retrieve_schema_objects(body.prompt, topk_tables=TOPK_OBJECTS)

    if not schema_hint.get("tables"):
        context = "### Objetos do DW selecionados\n(nenhum)\n\n### Amostra de dados\n(nenhuma)"
        answer = answer_natural_language(body.prompt, context, lang=body.lang)
        return {"answer": answer, "sources": [], "row_count": 0}

    try:
        sql = llm_generate_sql(body.prompt, schema_hint)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao gerar SQL seguro: {e}")

    try:
        rows = run_sql(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao executar consulta no DW: {e}")

    context = build_context(body.prompt, schema_hint, rows)
    answer = answer_natural_language(body.prompt, context, lang=body.lang)
    sources = [f"{t['ds']}.{t['table']}" for t in schema_hint["tables"]]

    return {
        "answer": answer,
        "sources": sources,
        "row_count": len(rows),
        "meta": {"objects_used": schema_hint["tables"], "limit": MAX_ROWS}
    }
