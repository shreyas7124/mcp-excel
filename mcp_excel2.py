#!/usr/bin/env python3
"""
excel_to_mcp.py

Extract Excel sheets to JSON (by schema if provided or best-effort),
call Groq LLM to convert tabular text to JSON, robustly parse the LLM output,
and optionally push results to a Modex Context Protocol (MCP) HTTP endpoint.

Requirements:
  pip install pandas openpyxl python-dotenv requests

Usage:
  export GROQ_API_KEY="..."
  export MCP_ENDPOINT="https://mcp.example/api/push"
  export MCP_API_KEY="..."
  python excel_to_mcp.py /path/to/file.xlsx --schema schema.json --model <model-name>
"""

import os
import re
import json
import argparse
import requests
from dotenv import load_dotenv
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_URL = os.getenv("GROQ_URL", "https://api.groq.com/openai/v1/chat/completions")
MCP_ENDPOINT = os.getenv("MCP_ENDPOINT")
MCP_API_KEY = os.getenv("MCP_API_KEY")


# -----------------------
# Excel text extraction
# -----------------------
def extract_text_from_excel_by_schema(excel_path: str, schema: Dict[str, Any]) -> Dict[str, str]:
    xls = pd.ExcelFile(excel_path)
    text_blocks = {}
    for sheet_key in schema.keys():
        matches = [s for s in xls.sheet_names if s.lower() == sheet_key.lower()]
        if not matches:
            print(f"[⚠] Sheet '{sheet_key}' not found in workbook. Skipping.")
            continue
        sheet_name = matches[0]
        df = pd.read_excel(xls, sheet_name=sheet_name)
        # expected schema: schema[sheet_key] = [ { field1: "type", field2:"type", ... } ]
        desired_cols = list(schema[sheet_key][0].keys()) if schema[sheet_key] else []
        # map with loose matching
        col_map = {}
        for col in desired_cols:
            for actual in df.columns:
                if col.lower().strip() in str(actual).lower().strip():
                    col_map[col] = actual
                    break
        filtered = pd.DataFrame()
        for col in desired_cols:
            if col in col_map:
                filtered[col] = df[col_map[col]]
            else:
                filtered[col] = None
        block = f"Sheet: {sheet_name}\n" + "\t".join(filtered.columns) + "\n"
        for _, row in filtered.iterrows():
            block += "\t".join([str(v) if pd.notnull(v) else "" for v in row]) + "\n"
        text_blocks[sheet_name] = block.strip()
    return text_blocks


def extract_text_from_excel_all_sheets(excel_path: str) -> Dict[str, str]:
    xls = pd.ExcelFile(excel_path)
    out = {}
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        block = f"Sheet: {sheet_name}\n"
        if not df.empty:
            block += "\t".join([str(c) for c in df.columns]) + "\n"
            for _, r in df.iterrows():
                block += "\t".join([str(c) if pd.notnull(c) else "" for c in r]) + "\n"
        else:
            block += "(Sheet empty)\n"
        out[sheet_name] = block.strip()
    return out


# -----------------------
# Prompt creation
# -----------------------
def create_prompt(sheet_name: str, text: str, fields_schema: Optional[List[Dict[str, Any]]] = None) -> str:
    # If schema provided, show it and request exact fields only
    if fields_schema:
        schema_block = json.dumps(fields_schema, indent=2)
        return f"""
You are an intelligent document parser.

Extract the table from the sheet '{sheet_name}' strictly matching the fields below.
Return ONLY valid JSON, no markdown, no explanations, or comments.
Exclude any fields not in the schema.

Schema for '{sheet_name}':
{schema_block}

Table:
\"\"\"{text}\"\"\"

Return only JSON in this format:
{{ "{sheet_name}": [ {{ "field1": value1, ... }} ] }}
"""
    else:
        return f"""
You are a JSON extraction agent.

Convert the following table data from sheet '{sheet_name}' into structured JSON.
Return ONLY valid JSON, no explanations or markdown.

Table:
\"\"\"{text}\"\"\"
"""


# -----------------------
# LLM call
# -----------------------
def call_groq_llm(prompt: str, model: str) -> str:
    if not GROQ_API_KEY:
        raise EnvironmentError("GROQ_API_KEY is not set.")
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
    }
    r = requests.post(GROQ_URL, headers=headers, json=payload)
    r.raise_for_status()
    parsed = r.json()
    # defensive: handle different response shapes
    if "choices" in parsed and parsed["choices"]:
        return parsed["choices"][0]["message"]["content"]
    if "text" in parsed:
        return parsed["text"]
    return json.dumps(parsed)


# -----------------------
# Robust JSON extraction
# -----------------------
def _find_top_level_json_blocks(s: str) -> List[Tuple[int, int]]:
    """
    Find indices (start, end) of top-level {...} or [...] JSON blocks using a stack
    so nested braces are handled correctly. Returns list of (start, end) slices.
    """
    starts = []
    blocks = []
    stack = []
    for i, ch in enumerate(s):
        if ch == '{' or ch == '[':
            stack.append((ch, i))
        elif ch == '}' or ch == ']':
            if not stack:
                continue
            opening, start_idx = stack.pop()
            # check matching pair type (simple)
            if (opening == '{' and ch == '}') or (opening == '[' and ch == ']'):
                if not stack:
                    # top-level block closed
                    blocks.append((start_idx, i + 1))
    return blocks


def _attempt_json_load(candidate: str) -> Any:
    """
    Try to load JSON, attempt small repairs:
     - remove trailing commas before } or ]
     - convert single quotes to double quotes (careful)
    """
    cand = candidate.strip()
    try:
        return json.loads(cand)
    except json.JSONDecodeError:
        # remove code fences
        cand = re.sub(r"^```(?:json)?\s*", "", cand, flags=re.IGNORECASE)
        cand = re.sub(r"\s*```$", "", cand, flags=re.IGNORECASE)
        # remove leading/explanatory lines like "Output:" or "JSON:"
        cand = re.sub(r'^[^\{\[\n]*\n', '', cand)
        # remove trailing commas before } or ]
        cand = re.sub(r",\s*([\]\}])", r"\1", cand)
        # try replace single-quotes with double only if it looks JSON-like with quotes
        if "'" in cand and '"' not in cand:
            cand2 = cand.replace("'", '"')
            try:
                return json.loads(cand2)
            except json.JSONDecodeError:
                pass
        try:
            return json.loads(cand)
        except json.JSONDecodeError:
            # give up
            raise


def extract_json_from_llm_output(raw_output: str) -> Any:
    """
    Returns parsed JSON object/array if possible.
    Otherwise returns {'raw_output': cleaned_text}.
    """
    if not raw_output:
        return {"raw_output": ""}

    # Remove markdown code fences and common noise
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw_output, flags=re.IGNORECASE | re.MULTILINE)
    cleaned = re.sub(r"\s*```$", "", cleaned, flags=re.IGNORECASE | re.MULTILINE)
    cleaned = cleaned.strip()

    # Try to detect top-level JSON blocks using bracket matching
    blocks = _find_top_level_json_blocks(cleaned)
    if not blocks:
        # fallback: search for FIRST curly or square-bracket block using regex heuristics
        m = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", cleaned)
        if not m:
            print("[❌] No JSON found in LLM output (no braces/brackets). Returning raw output.")
            return {"raw_output": cleaned}
        candidate = m.group(1)
        try:
            return _attempt_json_load(candidate)
        except Exception as e:
            print(f"[❌] Failed to parse candidate JSON: {e}")
            return {"raw_output": cleaned}

    # Try every block from last to first (often the final block is the JSON)
    for start, end in reversed(blocks):
        candidate = cleaned[start:end]
        try:
            return _attempt_json_load(candidate)
        except Exception:
            continue

    # If we couldn't parse any top-level block, return raw output for debugging
    print("[❌] Found JSON-like blocks but couldn't parse them. Returning raw output.")
    return {"raw_output": cleaned}


# -----------------------
# Push to MCP (generic)
# -----------------------
def push_to_mcp(mcp_endpoint: str, api_key: Optional[str], payload: Dict[str, Any]) -> Dict[str, Any]:
    if not mcp_endpoint:
        raise EnvironmentError("MCP_ENDPOINT not provided.")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    r = requests.post(mcp_endpoint, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return {"status": "ok", "raw_response_text": r.text}


# -----------------------
# Main orchestration
# -----------------------
def run_excel_extraction(path: str, schema_path: Optional[str], model: str,
                         push_to_mcp_flag: bool = False):
    schema = None
    if schema_path:
        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        with open(schema_path, "r") as f:
            schema = json.load(f)

    if schema:
        sheet_texts = extract_text_from_excel_by_schema(path, schema)
    else:
        sheet_texts = extract_text_from_excel_all_sheets(path)

    results = {}
    for sheet_name, block in sheet_texts.items():
        fields_schema = schema.get(sheet_name) if schema else None
        prompt = create_prompt(sheet_name, block, fields_schema)
        print(f"[i] Calling LLM for sheet '{sheet_name}'...")
        raw_output = call_groq_llm(prompt, model)
        parsed = extract_json_from_llm_output(raw_output)

        # Normalize into the final result structure:
        # If parsed is dict and contains the sheet_name key, use that value
        if isinstance(parsed, dict) and sheet_name in parsed:
            results[sheet_name] = parsed[sheet_name]
        else:
            results[sheet_name] = parsed

        # Optionally push to MCP per-sheet
        if push_to_mcp_flag:
            final_payload = {
                "file": os.path.basename(path),
                "sheet": sheet_name,
                "data": results[sheet_name]
            }
            try:
                resp = push_to_mcp(MCP_ENDPOINT, MCP_API_KEY, final_payload)
                print(f"[i] Pushed sheet '{sheet_name}' to MCP. Response: {resp}")
            except Exception as e:
                print(f"[⚠] Failed to push to MCP for sheet '{sheet_name}': {e}")

    full = {"file": os.path.basename(path), "results": results}
    print(json.dumps(full, indent=2, ensure_ascii=False))
    return full


# -----------------------
# CLI
# -----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="Path to Excel file")
    parser.add_argument("--schema", help="Optional schema JSON file path")
    parser.add_argument("--model", default="meta-llama/llama-4-maverick-17b-128e-instruct", help="LLM model")
    parser.add_argument("--push-to-mcp", action="store_true", help="Push results to MCP endpoint")
    args = parser.parse_args()

    run_excel_extraction(args.path, args.schema, args.model, push_to_mcp_flag=args.push_to_mcp)
