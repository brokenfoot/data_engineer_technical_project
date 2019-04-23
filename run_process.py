"""
Processes a gz file with json object on each line.
"""
import pandas as pd
import gzip
import json
from tqdm import tqdm
from sqlalchemy import create_engine
from datetime import date
from multiprocessing import Pool
import multiprocessing as mp
from html.parser import HTMLParser


class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)


def strip_tags(html):
    s = MLStripper()
    s.feed(html)

    return s.get_data()


def create_soc_mapping(onet_soc5_df, soc_hierarchy_df):
    final_soc_hierarchy_df = pd.DataFrame()
    for x in range(3, 6):
        temp_df = soc_hierarchy_df.loc[soc_hierarchy_df['level'] == str(x)]
        temp_df = temp_df[['child', 'parent']]
        temp_df.rename(columns = {
            "child": "soc%s" % x,
            "parent": "soc%s" % (x - 1)
        }, inplace = True)

        if final_soc_hierarchy_df.empty:
            final_soc_hierarchy_df = temp_df
        else:
            final_soc_hierarchy_df = final_soc_hierarchy_df.merge(temp_df, on = 'soc%s' % (x - 1), how = 'inner')

    final_df = onet_soc5_df.merge(final_soc_hierarchy_df, on = 'soc5', how = 'outer')
    final_df = final_df[['onet', 'soc5', 'soc2']]
    final_df['onet'].fillna('99-9999.99', inplace = True)
    final_df['soc5'].fillna('99-9999', inplace = True)
    final_df['soc2'].fillna('99-0000', inplace = True)

    return final_df


onet_soc5_df = pd.read_csv('map_onet_soc.csv', dtype = str)
soc_hierarchy_df = pd.read_csv('soc_hierarchy.csv', dtype = str)
onet_soc_mapping = create_soc_mapping(onet_soc5_df, soc_hierarchy_df)


def clean_html(data):
    clean_text = strip_tags(data['body'])
    # clean_text = re.sub('<[^<]+?>', '', clean_text)

    data['cleaned_html'] = clean_text

    return data


def add_soc_from_onet(data):
    temp_mapping = onet_soc_mapping.loc[onet_soc_mapping['onet'] == data['onet']]
    if temp_mapping.empty:
        print(data['onet'])

    for column in temp_mapping.columns:
        column_data = temp_mapping[column].tolist()
        data[column] = column_data[0]

    return data


def convert_dates_to_datetime(data):
    expired_date = [int(x) for x in data['expired'].split('-')]
    data['expired'] = date(expired_date[0], expired_date[1], expired_date[2])
    posted_date = [int(x) for x in data['posted'].split('-')]
    data['posted'] = date(posted_date[0], posted_date[1], posted_date[2])

    return data


def process_line(line):
    # load the line as a json object
    data = json.loads(line.decode("utf-8"))
    html_cleaned = clean_html(data)

    if html_cleaned['cleaned_html'] != data['body']:
        html_cleaned['fixed_html'] = 1
    else:
        html_cleaned['fixed_html'] = 0

    soc_added = add_soc_from_onet(html_cleaned)

    soc_added['body'] = soc_added['cleaned_html']
    del soc_added['cleaned_html']

    return soc_added


def main():
    engine = create_engine('sqlite:///db.sqlite3')

    in_file = gzip.open('sample.gz', 'rb')

    process_lines = []
    html_lines_cleaned = 0
    cpu_count = mp.cpu_count()
    pool = Pool(cpu_count - 2)
    for line in tqdm(in_file):
        process_lines.append(line)

        if len(process_lines) == cpu_count - 2:
            df = pd.DataFrame(pool.map(process_line, process_lines))
            df['expired'] = pd.to_datetime(df['expired'])
            df['posted'] = pd.to_datetime(df['posted'])
            html_lines_cleaned += df['fixed_html'].sum()

            df.drop('fixed_html', axis = 1, inplace = True)

            df.to_sql(
                name = 'data',
                con = engine,
                index = False,
                if_exists = 'append'
            )

            process_lines = []

    if len(process_lines) >= 1:
        df = pd.DataFrame(pool.map(process_line, process_lines))
        html_lines_cleaned += df['fixed_html'].sum()

        df.drop('fixed_html', axis = 1, inplace = True)

        df.to_sql(
            name = 'data',
            con = engine,
            index = False,
            if_exists = 'append'
        )

    sql = """
        SELECT
            COUNT(*) AS 'count'
        FROM data
    """
    df = pd.read_sql_query(sql, engine)

    print('total lines cleaned ->', df['count'].tolist()[0])
    print('records with html cleaned up ->', html_lines_cleaned)

    sql = """
        SELECT
            soc2,
            COUNT(*) AS 'doc_count'
        FROM data
        GROUP BY soc2
    """
    df = pd.read_sql_query(sql, engine)
    for row in df.iterrows():
        print(row[1]['soc2'], '->', row[1]['doc_count'])

    sql = """
        SELECT
            COUNT(*) AS 'doc_count'
        FROM data
        WHERE
            expired >= '2017-02-01' AND
            posted <= '2017-02-01'
    """
    df = pd.read_sql_query(sql, engine)
    print('postings active on Feb 1st, 2017 ->', df['doc_count'].tolist()[0])


if __name__ == '__main__':
    main()
