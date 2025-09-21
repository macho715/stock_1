
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from adapters.stock_adapter import build_stock_snapshots
from adapters.reporter_adapter import compute_flow_and_sqm
from adapters.invoice_adapter import run_invoice_validation_as_module
from hub.sku_master import build_sku_master, save_as_parquet_duckdb

def main():
    # Use local data files
    STOCK_EXCEL = r"HVDC_Stock On Hand Report.xlsx"
    INVOICE_SCRIPT = r"hvdc wh invoice.py"

    stock = build_stock_snapshots(STOCK_EXCEL)
    stock_summary = stock["summary_df"]

    stats = compute_flow_and_sqm()

    run_invoice_validation_as_module(INVOICE_SCRIPT)

    hub = build_sku_master(stock_summary, stats, invoice_match_df=None)
    pq = save_as_parquet_duckdb(hub)
    print(f"[OK] SKU_MASTER saved: {pq}")
    print(hub.head(10))

if __name__ == "__main__":
    main()
