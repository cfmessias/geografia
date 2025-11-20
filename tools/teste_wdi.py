import pandas as pd

df = pd.read_csv("data/wdi_sectors_wide.csv", sep=";")

iso = "DEU"  # ou PRT, FRA, ...
sub = df[df["iso3"] == iso].sort_values("year")

print(sub[["iso3", "year", "agr_vab", "ind_vab", "srv_vab"]])
print(sub[["iso3", "year", "agr_emp", "ind_emp", "srv_emp"]])
