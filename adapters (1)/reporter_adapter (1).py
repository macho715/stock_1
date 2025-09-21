
from hvdc_excel_reporter_final_sqm_rev import HVDCExcelReporterFinal
from pathlib import Path

def compute_flow_and_sqm() -> dict:
    rep = HVDCExcelReporterFinal()
    # Set data path to current directory where files are located
    rep.calculator.data_path = Path(".")
    # Use the available combined data file for both Hitachi and Siemens
    data_file = Path("HVDC_excel_reporter_final_sqm_rev.xlsx")
    rep.calculator.hitachi_file = data_file
    rep.calculator.simense_file = data_file  # Note: original code has typo "simense" not "siemens"
    
    print(f"📊 Using data file for both vendors: {data_file}")
    print(f"   - Hitachi file exists: {rep.calculator.hitachi_file.exists()}")
    print(f"   - Simense file exists: {rep.calculator.simense_file.exists()}")
    
    stats = rep.calculate_warehouse_statistics()
    return stats
