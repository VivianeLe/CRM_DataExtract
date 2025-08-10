import streamlit as st
import pandas as pd
from fpdf import FPDF
import base64

# ---------- helpers ----------
def df_to_pdf_table(pdf: FPDF, df: pd.DataFrame, col_widths=None, font_size=10):
    pdf.set_font("Arial", size=font_size)
    epw = pdf.w - 2*pdf.l_margin
    if col_widths is None:
        col_widths = [epw/len(df.columns)] * len(df.columns)

    # header
    pdf.set_fill_color(230, 230, 230)
    pdf.set_font("Arial", size=font_size, style="B")
    for w, col in zip(col_widths, df.columns):
        pdf.cell(w, 8, str(col), border=1, ln=0, fill=True, align="C")
    pdf.ln(8)
    pdf.set_font("Arial", size=font_size, style="")  # normal

    # rows
    for _, row in df.iterrows():
        for w, val in zip(col_widths, row):
            txt = "" if pd.isna(val) else str(val)
            pdf.cell(w, 7, txt, border=1, ln=0)
        pdf.ln(7)

def make_pdf(start_datetime, end_datetime, 
             summary_df: pd.DataFrame,
             stat_df: pd.DataFrame,
             by_Lottery: pd.DataFrame,
             by_ticket_segment: pd.DataFrame,
             by_inactive: pd.DataFrame) -> bytes:
    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()
    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, "CRM Campaign Report", ln=0)
    pdf.set_font("Arial", "", 10)
    pdf.cell(0, 10, f"From: {start_datetime}   To: {end_datetime}", ln=1, align="R")

    # Summary Result
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, "Overview", ln=1)

    df_to_pdf_table(pdf, summary_df, font_size=10)
    pdf.ln(4)

    # Ticket sold & Turnover statistic
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, "Ticket sold and Turnover statistic", ln=1)
    df_to_pdf_table(pdf, stat_df, font_size=10)
    pdf.ln(4)

    # By Lottery
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, "Ticket sold & Turnover by Lottery", ln=1)
    df_to_pdf_table(pdf, by_Lottery, font_size=10)
    pdf.ln(4)

    # By ticket segment
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, "Players by ticket sold", ln=1)
    df_to_pdf_table(pdf, by_ticket_segment, font_size=10)
    pdf.ln(4)

    # Reactivation result
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, "Reactivation result", ln=1)
    df_to_pdf_table(pdf, by_inactive, font_size=10)

    # return bytes
    pdf_bytes = pdf.output(dest="S").encode("latin-1")
    return pdf_bytes

# (Tuỳ chọn) dùng st.download_button:
# st.download_button("📄 Download PDF", data=pdf_bytes, file_name="summary_report.pdf", mime="application/pdf")
