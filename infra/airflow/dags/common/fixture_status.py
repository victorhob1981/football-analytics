FINAL_STATUSES = ("FT", "AET", "PEN", "FTP")
FINAL_STATUSES_SQL = ", ".join(f"'{status}'" for status in FINAL_STATUSES)

