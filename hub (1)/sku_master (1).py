
from dataclasses import dataclass
from typing import Optional
import pandas as pd
import duckdb

@dataclass
class SkuMasterRow:
    SKU: str
    hvdc_code_norm: Optional[str]
    vendor: Optional[str]
    pkg: Optional[float]
    gw: Optional[float]
    cbm: Optional[float]
    first_seen: Optional[str]
    last_seen: Optional[str]
    final_location: Optional[str]
    flow_code: Optional[int]
    flow_desc: Optional[str]
    stock_qty: Optional[float]
    sqm_cum: Optional[float]
    inv_match_status: Optional[str]
    err_gw: Optional[float]
    err_cbm: Optional[float]

def build_sku_master(stock_summary_df: pd.DataFrame, reporter_stats: dict,
                     invoice_match_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    s1 = stock_summary_df.rename(columns={
        "Warehouse": "last_wh",
        "First_Seen": "first_seen", "Last_Seen": "last_seen"
    })
    pick1 = [c for c in ["SKU","Status","first_seen","last_seen","Warehouse","Warehouse_Full_Name","Note"] if c in s1.columns]
    s1 = s1[pick1].copy() if pick1 else stock_summary_df.copy()

    dfp = reporter_stats.get("processed_data")
    if dfp is None or dfp.empty:
        raise RuntimeError("Reporter processed_data is empty")

    cols_map = {
        "Case No.":"SKU",
        "Pkg":"Pkg",
        "G.W(kgs)":"GW",
        "CBM":"CBM",
        "Vendor":"Vendor",
        "FLOW_CODE":"FLOW_CODE",
        "FLOW_DESCRIPTION":"flow_desc",
        "Final_Location":"Final_Location",
        "SQM":"SQM"
    }
    exist_cols = {k:v for k,v in cols_map.items() if k in dfp.columns}
    dfp2 = dfp[list(exist_cols.keys())].rename(columns=exist_cols)
    if "SKU" not in dfp2.columns:
        if "SKU" in dfp.columns:
            dfp2 = dfp.rename(columns={"G.W(kgs)":"GW"})
        else:
            raise RuntimeError("processed_data missing SKU/Case No. column.")

    inv = None
    if invoice_match_df is not None and not invoice_match_df.empty:
        inv = invoice_match_df.rename(columns={
            "Match_Status":"inv_match_status","Err_GW":"err_gw","Err_CBM":"err_cbm"
        })

    base = dfp2.drop_duplicates(subset=["SKU"]).copy()
    base["hvdc_code_norm"] = None
    if set(["first_seen","last_seen"]).issubset(s1.columns):
        base = base.merge(
            s1[["SKU","first_seen","last_seen"]], on="SKU", how="left"
        )
    if inv is not None:
        base = base.merge(inv[["SKU","inv_match_status","err_gw","err_cbm"]], on="SKU", how="left")

    base["sqm_cum"] = None

    want = ["SKU","hvdc_code_norm","Vendor","Pkg","GW","CBM","first_seen","last_seen",
            "Final_Location","FLOW_CODE","flow_desc","sqm_cum","inv_match_status","err_gw","err_cbm"]
    for c in want:
        if c not in base.columns:
            base[c] = None
    hub = base[want].copy()
    return hub

def save_as_parquet_duckdb(hub_df: pd.DataFrame, out_dir="out"):
    import os, pathlib
    pathlib.Path(out_dir).mkdir(exist_ok=True)
    
    # Ensure SKU column is string type to prevent conversion errors
    hub_df_copy = hub_df.copy()
    if 'SKU' in hub_df_copy.columns:
        hub_df_copy['SKU'] = hub_df_copy['SKU'].astype(str)
    
    pq = f"{out_dir}/SKU_MASTER.parquet"
    hub_df_copy.to_parquet(pq, index=False)
    con = duckdb.connect(database=f"{out_dir}/sku_master.duckdb")
    con.execute("CREATE TABLE IF NOT EXISTS sku_master AS SELECT * FROM read_parquet(?)", [pq])
    con.close()
    return pq
