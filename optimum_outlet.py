import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta

today = datetime.today()
m1 = (today - relativedelta(months=1)).strftime('%Y-%m-%d')
m1_yearmonth = datetime.strptime(m1, '%Y-%m-%d').strftime('%Y%m')

# Dataset and cleansing
df = pd.read_csv("optimum_outlet_dataset_{}.csv.gz".format(m1_yearmonth), compression="gzip")
df["avg_rev"] = df["rev_active_traditional_channel"] / df["active_outlet_num"]
df["avg_rev"].fillna(0, inplace=True)
df = df[df["archetype"]!=0]

# Model only for site with active outlets
df_new = df[df["active_outlet_num"]>0]

# Remove outlier
Q1 = df_new.quantile(0.25)
Q3 = df_new.quantile(0.90)
IQR = Q3 - Q1

df_loop = df_new[~((df_new < (Q1 - 1.5 * IQR)) |(df_new > (Q3 + 1.5 * IQR))).any(axis=1)]
df_loop = df_loop.groupby(["active_outlet_num","archetype","kabupaten"], as_index=False).agg({"avg_rev":"mean"})

# 1st loop, get optimum outlet per archetype and per city
kabupaten = df_loop["kabupaten"].unique()

optimum_per_city=[]
for i in kabupaten:
    df_filter = df_loop[df_loop["kabupaten"]=="{}".format(i)]
    archetype = df_filter["archetype"].unique()
    for j in archetype:
        df_filter2 = df_filter[df_filter["archetype"]=="{}".format(j)]
        df_fin = df_filter2[df_filter2["avg_rev"]==df_filter2["avg_rev"].max()]
        optimum_per_city.append(df_fin)

df_optimum_per_city = pd.concat(optimum_per_city)
df_optimum_per_city = df_optimum_per_city.drop(["avg_rev"],axis=1)
df_optimum_per_city = df_optimum_per_city[["archetype","kabupaten","active_outlet_num"]]
df_optimum_per_city = df_optimum_per_city.rename({'active_outlet_num': 'optimum_outlet'}, axis=1)

# Assign optimum outlet number to dataset
df_final = df.merge(df_optimum_per_city, on=["archetype","kabupaten"], how="left").fillna({"optimum_outlet":0})

# 2nd loop, labelling site with outlet category
kabupaten = df_final["kabupaten"].unique()

cat_per_city=[]
for i in kabupaten:
    df_filter = df_final[df_final["kabupaten"]=="{}".format(i)]
    archetype = df_filter["archetype"].unique()
    for j in archetype:
        df_filter2 = df_filter[df_filter["archetype"]=="{}".format(j)]
        df_filter2["avg_rev_city_arch"] = df_filter2["rev_active_traditional_channel"].mean()
        df_filter2["outlet_sub_category"] = np.where((df_filter2['delta_to_optimum_outlet']>0) & (df_filter2['rev_active_traditional_channel']>df_filter2['avg_rev_city_arch']), "Non Optimum Outlet Rev Above Avg",
            (np.where((df_filter2['delta_to_optimum_outlet']>0) & (df_filter2['rev_active_traditional_channel']<=df_filter2['avg_rev_city_arch']), "Non Optimum Outlet Rev Below Avg",
            (np.where((df_filter2['delta_to_optimum_outlet']<=0) & (df_filter2['rev_active_traditional_channel']>df_filter2['avg_rev_city_arch']), "Optimum Outlet Rev Above Avg",
            (np.where((df_filter2['delta_to_optimum_outlet']<=0) & (df_filter2['rev_active_traditional_channel']<=df_filter2['avg_rev_city_arch']), "Optimum Outlet Rev Below Avg","Unknown")))))))
        cat_per_city.append(df_filter2)

df_finals = pd.concat(cat_per_city)

df_finals["outlet_category"] = np.where((df_finals['outlet_sub_category']=="Non Optimum Outlet Rev Above Avg") |
                                        (df_finals['outlet_sub_category']=="Non Optimum Outlet Rev Below Avg"), 
                                        "Non Optimum Outlet","Optimum Outlet")

df_finals.to_csv('optimum_outlet_per_city_{}.csv.gz'.format(m1_yearmonth), header=True, index=False, compression="gzip")
