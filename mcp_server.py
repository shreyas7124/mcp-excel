# mcp_server.py
from fastmcp import FastMCP
from typing import List, Dict, Any, Optional
import pandas as pd
import os
import json
import sys

app = FastMCP("excel-data")

# In-memory stores
DB: List[Dict[str, Any]] = []              # JSON-serializable store (headers + data)
DATAFRAMES: Dict[str, pd.DataFrame] = {}   # real DataFrames for computations
LOADED_FILE: Optional[str] = None          # path to current loaded file
SCHEMA: Optional[Dict[str, Any]] = None    # optional schema provided


# ----------------------
# Helpers
# ----------------------
def _coerce_df_by_schema(df: pd.DataFrame, schema_for_sheet: Dict[str, str]) -> pd.DataFrame:
    """Coerce columns in df according to schema_for_sheet mapping: {field_name: type_str}"""
    df = df.copy()
    for field, typ in schema_for_sheet.items():
        # find best-matching column in df (loose match)
        col_match = None
        for c in df.columns:
            if field.lower().strip() in str(c).lower().strip():
                col_match = c
                break
        if col_match is None:
            continue
        try:
            if typ.lower() in ("int", "integer"):
                df[col_match] = pd.to_numeric(df[col_match], errors="coerce").fillna(0).astype(int)
            elif typ.lower() in ("float", "double", "number"):
                df[col_match] = pd.to_numeric(df[col_match], errors="coerce").astype(float)
            elif typ.lower() in ("datetime", "date", "time"):
                df[col_match] = pd.to_datetime(df[col_match], errors="coerce")
            else:
                df[col_match] = df[col_match].astype(str)
        except Exception:
            # best-effort: leave original if coercion fails
            pass
    return df


def _add_sheet_to_db(file_path: str, sheet_name: str, df: pd.DataFrame):
    rec = {
        "file": os.path.basename(file_path),
        "sheet": sheet_name,
        "data": {
            "headers": list(df.columns),
            "totalRows": int(len(df)),
            "totalColumns": int(len(df.columns)),
            "sheetName": sheet_name,
            "data": df.to_dict(orient="records"),
        },
    }
    DB.append(rec)


# ----------------------
# Tools exposed to Claude
# ----------------------
@app.tool()
def load_file(file_path: str, schema_json: Optional[str] = None) -> Dict[str, Any]:
    """
    Load an Excel file from local disk into memory.
    - file_path: absolute path to .xlsx (must be readable by the server process)
    - schema_json: optional JSON string or path to JSON file describing expected schema:
        { "Sheet1": [ { "colA": "int", "colB": "string" } ], "Sheet2": [...] }
    Returns status and list of sheets loaded.
    """
    global LOADED_FILE, DATAFRAMES, DB, SCHEMA
    if not os.path.exists(file_path):
        return {"status": "error", "message": f"File not found: {file_path}"}

    # parse schema if provided
    SCHEMA = None
    if schema_json:
        # try parse as JSON string; if fails and it's a path, try load file
        try:
            SCHEMA = json.loads(schema_json)
        except Exception:
            if os.path.exists(schema_json):
                with open(schema_json, "r", encoding="utf-8") as f:
                    SCHEMA = json.load(f)
            else:
                return {"status": "error", "message": "schema_json is not valid JSON nor a file path."}

    # clear previous state
    DB = []
    DATAFRAMES = {}
    LOADED_FILE = file_path

    # read all sheets
    try:
        excel_data = pd.read_excel(file_path, sheet_name=None)
    except Exception as e:
        return {"status": "error", "message": f"Failed to read Excel: {e}"}

    for sheet_name, df in excel_data.items():
        # apply schema coercion if schema provided for this sheet
        if SCHEMA and sheet_name in SCHEMA:
            schema_for_sheet = SCHEMA[sheet_name][0] if isinstance(SCHEMA[sheet_name], list) and len(SCHEMA[sheet_name])>0 else {}
            df_coerced = _coerce_df_by_schema(df, schema_for_sheet)
        else:
            df_coerced = df.copy()

        DATAFRAMES[sheet_name] = df_coerced
        _add_sheet_to_db(file_path, sheet_name, df_coerced)

    return {
        "status": "ok",
        "message": f"Loaded {len(DATAFRAMES)} sheet(s) from {file_path}",
        "sheets": list(DATAFRAMES.keys()),
        "rows": {s: int(len(DATAFRAMES[s])) for s in DATAFRAMES},
    }


@app.tool()
def list_sheets() -> List[str]:
    """Return list of loaded sheet names."""
    return list(DATAFRAMES.keys())


@app.tool()
def describe(sheet: Optional[str] = None) -> Any:
    """Return headers and a small sample for a sheet (or for all if sheet is None)."""
    if not DATAFRAMES:
        return {"status": "error", "message": "No file loaded. Call load_file first."}
    if sheet:
        if sheet not in DATAFRAMES:
            return {"status": "error", "message": f"Sheet '{sheet}' not loaded."}
        df = DATAFRAMES[sheet]
        return {
            "sheet": sheet,
            "headers": list(df.columns),
            "rows": int(len(df)),
            "sample": df.head(10).to_dict(orient="records"),
        }
    # describe all
    out = {}
    for s, df in DATAFRAMES.items():
        out[s] = {"headers": list(df.columns), "rows": int(len(df)), "sample": df.head(3).to_dict(orient="records")}
    return out


@app.tool()
def get_rows(sheet: Optional[str] = None, q: Optional[str] = None, limit: int = 20) -> List[Dict]:
    """
    Return rows from sheet (or all sheets) that contain q (case-insensitive).
    If q is None returns first `limit` rows for the sheet.
    """
    if not DATAFRAMES:
        return []
    results = []
    def _row_matches(row: Dict, q_lower: str) -> bool:
        for v in row.values():
            if v is None:
                continue
            if q_lower in str(v).lower():
                return True
        return False

    if sheet:
        if sheet not in DATAFRAMES:
            return []
        df = DATAFRAMES[sheet]
        if not q:
            return df.head(limit).to_dict(orient="records")
        ql = q.lower()
        matches = [r for r in df.to_dict(orient="records") if _row_matches(r, ql)]
        return matches[:limit]

    # search across all sheets
    ql = (q or "").lower()
    for s, df in DATAFRAMES.items():
        if not q:
            chunk = df.head(limit).to_dict(orient="records")
            for r in chunk:
                results.append({"sheet": s, "row": r})
        else:
            for r in df.to_dict(orient="records"):
                if _row_matches(r, ql):
                    results.append({"sheet": s, "row": r})
                    if len(results) >= limit:
                        return results
    return results[:limit]


@app.tool()
def aggregate(sheet: str, group_by: Optional[str], agg_col: str, agg_func: str = "sum") -> Any:
    """
    Simple aggregation helper.
    - sheet: sheet name to aggregate
    - group_by: column name to group by (if None, aggregate over entire sheet)
    - agg_col: column to aggregate (must be numeric)
    - agg_func: one of ["sum","mean","min","max","count"]
    """
    if sheet not in DATAFRAMES:
        return {"status": "error", "message": f"Sheet '{sheet}' not loaded."}
    df = DATAFRAMES[sheet]
    if agg_col not in df.columns:
        return {"status": "error", "message": f"Aggregation column '{agg_col}' not found in sheet."}
    try:
        if group_by and group_by in df.columns:
            res = df.groupby(group_by)[agg_col].agg(agg_func).reset_index()
            return {"result": res.to_dict(orient="records")}
        else:
            val = getattr(df[agg_col], agg_func)()
            return {"result": float(val)}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# keep a generic push that accepts JSON payloads (optional; for http mode)
@app.tool()
def push_json(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Accept pre-structured JSON (from an external script) and store it.
    payload expected to contain 'file', 'sheet', 'data' keys.
    """
    try:
        DB.append(payload)
        return {"status": "ok", "count": len(DB)}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ----------------------
# Run
# ----------------------
if __name__ == "__main__":
    # If run with --http, start as HTTP server (useful if you want to POST from your script)
    if "--http" in sys.argv:
        # Example: python mcp_server.py --http  -> runs HTTP server on default port
        app.run("http", host="0.0.0.0", port=8000)
    else:
        # default run (stdio) — Claude Desktop expects this mode
        app.run()
