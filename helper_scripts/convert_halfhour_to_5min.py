import argparse
import pandas as pd


def arg_parser():
    description = ("Convert half hour trace data into 5 min interpolated data")
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-path', type=str, required=True,
                        help='Path to file.')
    parser.add_argument('-year', type=int, required=True,
                        help='Year to filter, if provided')
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = arg_parser()
    year = args.year
    df = pd.read_csv(args.path)
    df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']])
    df = df.drop(['Year', 'Month', 'Day'], axis=1)
    # columns should be numeric from 1 to 48 (half hours)
    pivot = df.melt(id_vars='Date', var_name='HH', value_name='gen_mw')
    pivot['HH'] = pivot['HH'].astype("float64")
    pivot['HH'] = pivot['HH'].apply(lambda x: pd.Timedelta(minutes=30 * x))
    pivot['Datetime'] = pivot['Date'] + pivot['HH']
    pivot = pivot.sort_values('Datetime')
    pivot.drop(columns=['Date', 'HH'], inplace=True)
    pivot.set_index('Datetime', inplace=True)
    start = f'{year}-01-01 00:05:00'
    end = f'{year + 1}-01-01 00:00:00'
    pivot = pivot[start:end]
    if pivot.index[0].minute != 5:
        bfill_daterange = pd.date_range(start=f'{year}-01-01 00:05:00',
                                        end=(pivot.index[0] - 
                                             pd.Timedelta(minutes=5)), 
                                        freq='5T')
        bfill = pd.DataFrame(data=[pivot.iloc[0, 0]] * len(bfill_daterange),
                             index=bfill_daterange, columns=['gen_mw'])
        pivot = pd.concat([bfill, pivot], axis=0)
        pivot.index.name = 'Datetime'
    resampled = pivot.asfreq('5T')
    resampled.interpolate().to_csv(f'{args.path}_5mininterpolated.csv')
