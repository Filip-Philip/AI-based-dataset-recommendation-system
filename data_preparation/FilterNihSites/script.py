import pandas as pd
data = pd.read_csv("download.csv")
extracted = data[data['Subject_Area'].str.contains("Neuroscience")]
extracted.to_csv("filtered.csv")
extracted = data.drop(data.columns.difference(['Repository_name','Subject_Area','Data_Access_Url','Repository_Url']),axis=1)
extracted.to_csv("filtered_minimal.csv")


print(extracted['Repository_Url'].to_clipboard())