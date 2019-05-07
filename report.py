#!/opt/local/anaconda3/bin/python

import pandas as pd
import argparse


class CommandLine:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description="Description for my parser")
        self.parser.add_argument('file', type=argparse.FileType('r'), metavar='FILE',
                                 help='an integer for the accumulator')
        self.parser.add_argument("-H", "--Help",
                                 help="This file is used to create reports of users or accounts that state "
                                      "ressource consumptions.\nUSAGE:\npython report.py -p cpu -c User "
                                      "file", required=False, default="")
        self.parser.add_argument("-p", "--partition", type=str, help="Choose the partition you want to filter by",
                                 required=False, default="")
        self.parser.add_argument("-c", "--category", type=str,
                                 help="Choose the category you wnat to group by (currently 'User' or "
                                      "'Account')", default="User")

        self.argument = self.parser.parse_args()
        self.status = False

        if self.argument.Help:
            print("You have used '-H' or '--Help' with argument: {0}".format(self.argument.Help))
            self.status = True
        if self.argument.partition:
            print("You have used '-p' or '--partition' with argument: {0}".format(self.argument.partition))
            self.status = True
        if self.argument.category:
            print("You have used '-c' or '--category' with argument: {0}".format(self.argument.category))
            self.status = True
        if not self.status:
            print("Maybe you want to use -H or -p or -c as arguments ?")


def include_batches_in_job(df):
    # type: (pd.DataFrame) -> pd.DataFrame
    batches = df[df['JobID'].str.contains(".batch")]
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
    return filtered_df


def num_with_letter(x):
    if isinstance(x, str):
        if x == '0':
            return x
        else:
            return x[:-1]
    else:
        return x


def format_data(df):
    # type: (pd.DataFrame) -> pd.DataFrame
    df['MaxRSS(K)'] = df['MaxRSS(K)'].apply(lambda x: num_with_letter(x))
    df['MaxDiskWrite(M)'] = df['MaxDiskWrite(M)'].apply(lambda x: num_with_letter(x))
    df['MaxDiskRead(M)'] = df['MaxDiskRead(M)'].apply(lambda x: num_with_letter(x))
    df['MaxVMSize(K)'] = df['MaxVMSize(K)'].apply(lambda x: num_with_letter(x))
    df['MaxPages(K)'] = df['MaxPages(K)'].apply(lambda x: num_with_letter(x))
    df['ReqMem(per Node)'] = df['ReqMem(per Node)'].apply(lambda x: num_with_letter(x))

    # change data types
    df[['JobID', 'JobIDRaw', 'NTasks', 'AllocCPUS', 'MaxRSS(K)', 'MaxDiskWrite(M)', 'MaxDiskRead(M)', 'MaxVMSize(K)',
        'MaxPages(K)', 'CPUTimeRAW', 'ReqMem(per Node)']] = df[['JobID', 'JobIDRaw', 'NTasks', 'AllocCPUS',
                                                                'MaxRSS(K)', 'MaxDiskWrite(M)',
                                                                'MaxDiskRead(M)', 'MaxVMSize(K)',
                                                                'MaxPages(K)', 'CPUTimeRAW',
                                                                'ReqMem(per Node)']].apply(pd.to_numeric,
                                                                                           errors='coerce')
    df[['AveCPU', 'Elapsed', 'CPUTime']] = df[['AveCPU', 'Elapsed', 'CPUTime']].apply(pd.to_timedelta,
                                                                                      errors='coerce')
    df[['Submit', 'Start', 'End']] = df[['Submit', 'Start', 'End']].apply(pd.to_datetime)

    # calculate total time
    # https://github.com/CSCfi/slurm-stats/blob/master/sacct_stats.R
    # Calculate the total time (core seconds) for each job
    # dt$TotalTime=dt$Elapsed * dt$AllocCPUS
    df['Elapsed Calculated'] = df['End'] - df['Start']
    df['TotalTime (core seconds)'] = df['Elapsed Calculated'] * df['AllocCPUS']
    return df


# TODO: argument if partition which
def filter_partition(df, partition):
    # type: (pd.DataFrame, str) -> pd.DataFrame
    if partition is not None:
        df = df[df['Partition'] == partition]
    return df


# TODO: argument: per user, per account
def report_by_x(df, by_x):
    # type: (pd.DataFrame, str) -> pd.DataFrame
    """

    :param df:
    :param by_x: valid parameters: 'User', 'Account'
    :return:
    """

    df_by_x = df.groupby(by_x).agg({'NTasks': 'sum',
                                    'AllocCPUS': 'sum',
                                    'AveCPU': 'sum',
                                    'MaxDiskRead(M)': 'mean',
                                    'MaxDiskWrite(M)': 'mean',
                                    'Elapsed': 'sum',
                                    'Elapsed Calculated': 'sum',
                                    'CPUTime': 'sum',
                                    'TotalTime (core seconds)': 'sum'
                                    })

    # last minute data formatting
    df_by_x['MaxDiskRead (GB)'] = df_by_x['MaxDiskRead(M)'] / float(1000)
    df_by_x['MaxDiskWrite (GB)'] = df_by_x['MaxDiskWrite(M)'] / float(1000)
    df_by_x = df_by_x.drop(columns=['MaxDiskRead(M)', 'MaxDiskWrite(M)', 'Elapsed'])
    return df_by_x


if __name__ == "__main__":
    app = CommandLine()
    print(app.argument)
    partition = app.argument.partition
    by_x = app.argument.category
    accounting_filename = app.argument.file
    accounting_data = pd.read_csv(accounting_filename, delimiter='|')
    accounting_data = include_batches_in_job(accounting_data)
    accounting_data = format_data(accounting_data)
    accounting_data = filter_partition(accounting_data, partition)
    accounting_data = report_by_x(accounting_data, by_x)
    output_name = partition + "_" + by_x + "_report"
    accounting_data.to_csv(path_or_buf=output_name)
