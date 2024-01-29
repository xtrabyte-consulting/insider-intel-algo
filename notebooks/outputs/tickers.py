recent_tickers_file = "recent_tickers_traded.txt"
value_over_median_file = "tickers_value_over_median.txt"

# Read the contents of the files
with open(recent_tickers_file, "r") as file:
  recent_tickers = file.read().splitlines()

with open(value_over_median_file, "r") as file:
  value_over_median_tickers = file.read().splitlines()

# Convert the tickers into sets
recent_tickers_set = set(recent_tickers)
value_over_median_tickers_set = set(value_over_median_tickers)

# Find the tickers that are in recent_tickers but not in value_over_median_tickers
tickers_not_in_value_over_median = recent_tickers_set - value_over_median_tickers_set

# Print the resulting tickers
for ticker in tickers_not_in_value_over_median:
  print(ticker)
  
with open("missing_tickers.txt", "w") as file:
  file.write("\n".join(tickers_not_in_value_over_median))