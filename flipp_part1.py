import pandas as pd
import numpy as np
import datetime

# Import Data
df = pd.read_csv("Data Engineer Take home dataset - dataset.csv")

# Convert timestamp from string to timestamp type
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Define regex pattern later used to filter invalid flyer_id and merchant_id
regex_pattern = r'[0-9]+'

# Initialize output dataframe
output_user = pd.DataFrame(columns=["avg_flyer_time"])
output_flyer = pd.DataFrame(columns=["flyer_id", "merchant_id", "duration_sum", "event_count"])
output_flyer = output_flyer.set_index(['flyer_id', 'merchant_id'])

# Algorithm used to calculate the average time on flyer per user and aggregate all flyer events into one df
for user_id in df.user_id.unique():
    df_user = df[df["user_id"] == user_id].sort_values(by=['timestamp'])
    df_user['timestamp_next'] = df_user['timestamp'].shift(-1)
    df_user['duration'] = df_user['timestamp_next'] - df_user['timestamp']
    df_user.iloc[-1, df_user.columns.get_loc('duration')] = datetime.timedelta(minutes=15)
    df_user_filtered = df_user[(df_user.duration <= datetime.timedelta(minutes=15)) &
            (df_user.flyer_id.str.match(regex_pattern)) &
            (df_user.merchant_id.str.match(regex_pattern)) &
            ((df_user.event == "flyer_open") | (df_user.event == "item_open")) ]
    df_flyer_agg = df_user_filtered.groupby(["flyer_id", "merchant_id"]).agg({"duration":['sum'], "event": ['count']})
    df_flyer_agg.columns = ["_".join(x) for x in df_flyer_agg.columns.ravel()]

    output_user.loc[user_id] = df_user_filtered.duration.mean()
    output_flyer = output_flyer.append(df_flyer_agg)

# Group by flyer_id and merchant_id to calculate total duration, number of events and average duration 
output_flyer = output_flyer.groupby(["flyer_id", "merchant_id"]).agg(sum)
output_flyer["duration_avg"] = output_flyer.duration_sum/output_flyer.event_count
