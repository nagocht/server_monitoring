import os
import datetime
import numpy as np
import math
import pandas as pd

# start = datetime.datetime.strptime('2019-04-15T23:51:42', '%Y-%m-%dT%H:%M:%S')
# end = datetime.datetime.strptime('2019-04-16T08:32:36', '%Y-%m-%dT%H:%M:%S')
# print(start, end, end-start)

df = pd.read_csv('sisu', delimiter='|')

print(df.columns)

batches = df[df['JobID'].str.contains(".batch")]
display(batches)

for index, row in batches.iterrows():
    #     print(row['JobID'])
    job_id = row['JobID'].split('.')[0]
    aveCPU = row['AveCPU']
    cpuTime = row['CPUTime']  # can be different batch to job
    cpuTimeRaw = row['CPUTimeRAW']  # can be different batch to job
    elapsed = row['Elapsed']  # can be different batch to job
    end = row['End']  # can be different batch to job

    maxDiskWrite = row['MaxDiskWrite']
    maxDiskRead = row['MaxDiskRead']
    maxPages = row['MaxPages']
    maxRSS = row['MaxRSS']
    maxVMSize = row['MaxVMSize']
    nTasks = row['NTasks']

    df.loc[df.JobID == job_id, 'AveCPU'] = aveCPU
    df.loc[df.JobID == job_id, 'CPUTime'] = cpuTime
    df.loc[df.JobID == job_id, 'CPUTimeRAW'] = cpuTimeRaw
    df.loc[df.JobID == job_id, 'Elapsed'] = elapsed
    df.loc[df.JobID == job_id, 'End'] = end

    df.loc[df.JobID == job_id, 'MaxDiskWrite'] = maxDiskWrite
    df.loc[df.JobID == job_id, 'MaxDiskRead'] = maxDiskRead
    df.loc[df.JobID == job_id, 'MaxRSS'] = maxRSS
    df.loc[df.JobID == job_id, 'MaxPages'] = maxPages
    df.loc[df.JobID == job_id, 'MaxVMSize'] = maxVMSize
    df.loc[df.JobID == job_id, 'NTasks'] = nTasks

filtered_df = df[~df.JobID.str.contains(".batch")]
filtered_df.rename(columns={'MaxRSS': 'MaxRSS(K)',
                            'MaxDiskWrite': 'MaxDiskWrite(M)',
                            'MaxDiskRead': 'MaxDiskRead(M)',
                            'MaxVMSize': 'MaxVMSize(K)',
                            'MaxPages': 'MaxPages(K)',
                            'ReqMem': 'ReqMem(per Node)'
                            },
                   inplace=True)


def num_with_letter(x):
    if isinstance(x, str):
        if x == '0':
            return x
        else:
            return x[:-1]
    else:
        return x


filtered_df['MaxRSS(K)'] = filtered_df['MaxRSS(K)'].apply(lambda x: num_with_letter(x))
filtered_df['MaxDiskWrite(M)'] = filtered_df['MaxDiskWrite(M)'].apply(lambda x: num_with_letter(x))
filtered_df['MaxDiskRead(M)'] = filtered_df['MaxDiskRead(M)'].apply(lambda x: num_with_letter(x))
filtered_df['MaxVMSize(K)'] = filtered_df['MaxVMSize(K)'].apply(lambda x: num_with_letter(x))
filtered_df['MaxPages(K)'] = filtered_df['MaxPages(K)'].apply(lambda x: num_with_letter(x))
filtered_df['ReqMem(per Node)'] = filtered_df['ReqMem(per Node)'].apply(lambda x: num_with_letter(x))

# change data types
filtered_df[
    ['JobID', 'JobIDRaw', 'NTasks', 'AllocCPUS', 'MaxRSS(K)', 'MaxDiskWrite(M)', 'MaxDiskRead(M)', 'MaxVMSize(K)',
     'MaxPages(K)', 'CPUTimeRAW', 'ReqMem(per Node)']] = filtered_df[['JobID', 'JobIDRaw', 'NTasks', 'AllocCPUS',
                                                                      'MaxRSS(K)', 'MaxDiskWrite(M)',
                                                                      'MaxDiskRead(M)', 'MaxVMSize(K)',
                                                                      'MaxPages(K)', 'CPUTimeRAW',
                                                                      'ReqMem(per Node)']].apply(pd.to_numeric,
                                                                                                 errors='coerce')
filtered_df[['AveCPU', 'Elapsed', 'CPUTime']] = filtered_df[['AveCPU', 'Elapsed', 'CPUTime']].apply(pd.to_timedelta,
                                                                                                    errors='coerce')
filtered_df[['Submit', 'Start', 'End']] = filtered_df[['Submit', 'Start', 'End']].apply(pd.to_datetime)

# calculate total time
# https://github.com/CSCfi/slurm-stats/blob/master/sacct_stats.R
# Calculate the total time (core seconds) for each job
# dt$TotalTime=dt$Elapsed * dt$AllocCPUS
filtered_df['Elapsed Calculated'] = filtered_df['End'] - filtered_df['Start']
filtered_df['TotalTime (core seconds)'] = filtered_df['Elapsed Calculated'] * filtered_df['AllocCPUS']

# TODO: argument if partion which
cpu = filtered_df[filtered_df['Partition'] == 'cpu']
gpu = filtered_df[filtered_df['Partition'] == 'gpu']
print(filtered_df.columns)

# TODO: argument: per user, per account
per_user = cpu.groupby('User').agg({'NTasks': 'sum',
                                    'AllocCPUS': 'sum',
                                    'AveCPU': 'sum',
                                    'MaxDiskRead(M)': 'mean',
                                    'MaxDiskWrite(M)': 'mean',
                                    'Elapsed': 'sum',
                                    'Elapsed Calculated': 'sum',
                                    'CPUTime': 'sum',
                                    'TotalTime (core seconds)': 'sum'
                                    })

per_user['MaxDiskRead (GB)'] = per_user['MaxDiskRead(M)'] / float(1000)
per_user['MaxDiskWrite (GB)'] = per_user['MaxDiskWrite(M)'] / float(1000)
per_user = per_user.drop(columns=['MaxDiskRead(M)', 'MaxDiskWrite(M)', 'Elapsed'])

display(per_user)
