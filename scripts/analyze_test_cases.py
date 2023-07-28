import os
import sys
import argparse
import sqlparse
SCRIPDIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPDIR))
from src import testanalyzer
from src import testcollector
from src import testparser
import matplotlib.pyplot as plt
from matplotlib import rc
import pandas as pd
import numpy as np

Supported_DBMS = ['SQLite', 'MySQL', 'DuckDB', 'CockroachDB', 'PostgreSQL']
Image_Dir = "../SQuaLity-Paper/assets/img"
Table_Dir = "../SQuaLity-Paper/assets/table"

def plot_test_case_length(db_names = Supported_DBMS, output:str = Image_Dir):
    db_dict = {}
    parser = testparser.Parser()
    for db_name in db_names:
        files = testcollector.find_local_tests(db_name)
        file_lengths = []
        
        # Note here we count the test file for MySQL and PostgreSQL
        for file in files:
            # get the length of the file and append it to the list
            try:
                file_length = len(parser.read_file(file, byline=True))
            except UnicodeDecodeError:
                # print(f"UnicodeDecodeError: {file}")
                continue
            # if db_name == "MySQL" and file_length > 5000:
            #     print(f"file_length: {file_length}, file: {file}")
            file_lengths.append(file_length)
            db_dict[db_name] = file_lengths
    # begin plotting
    plt.boxplot(db_dict.values(), labels=db_dict.keys())
    plt.yscale('log')
    plt.ylabel("Length of Test Cases")
    # plt.show()
    plt.savefig(os.path.join(output, "test_case_length.svg"))

def generate_test_case_data(db_names = Supported_DBMS, input:str = 'data/',output:str = Table_Dir):
    db_dict = {}
    standard_percentage_perfile = []
    standard_percentage_overall = []
    for db_name in db_names:
        db_dict[db_name] = {}
        analyzer = testanalyzer.TestCaseAnalyzer()
        print(f"Processing {db_name}")
        # data we need to collect
        record_counts = []
        
        test_case_path = os.path.join(input, db_name)
        analyzer.load_testcases(test_case_path)
        # get total test cases
        print(f"Total test cases: {analyzer.test_num}")
        analyzer.test_cases = analyzer.test_cases[analyzer.test_cases['TYPE'] != 'CONTROL']
        analyzer.test_cases['SQL_TYPE']  = analyzer.test_cases.apply(lambda row: analyzer.get_sql_statement_type(row['SQL'].lstrip()), axis=1)
        analyzer.test_cases['IS_STANDARD'] = analyzer.test_cases.apply(lambda row: analyzer.is_standard(row['SQL'].lstrip(' (“$')), axis=1)
        sql_type_count = analyzer.test_cases['SQL_TYPE'].dropna().value_counts(normalize=True)
        sql_type_count.rename(None, inplace=True)
        db_dict[db_name]['sql_type_count'] = sql_type_count
        print(f"SQL_TYPE: {sql_type_count}")
        # count the number of standard files
        standard_test_files = analyzer.test_cases.groupby('TESTFILE_PATH').filter(lambda x: x['IS_STANDARD'].all())['TESTFILE_PATH']
        standard_test_files_num = standard_test_files.nunique()
        for file in standard_test_files.unique():
            print(file)
        print(f"Number of standard files: {standard_test_files_num}")
        standard_percentage_perfile.append(standard_test_files_num / analyzer.test_cases['TESTFILE_PATH'].nunique())
        
        overall_standard_cases = analyzer.test_cases['IS_STANDARD'].sum()
        standard_percentage = overall_standard_cases / analyzer.test_num
        print(f"Percentage of standard cases: {standard_percentage}")
        standard_percentage_overall.append(standard_percentage)
        
        # plot info of SQL_TYPE and savefig
        sql_type_count[:10].plot(kind='bar')
        plt.tight_layout()
        plt.xticks(rotation=45, ha='right')
        plt.savefig(os.path.join(output, f"{db_name}_sql_type.png"))
        sql_type_count.to_csv(os.path.join(output, f"{db_name}_sql_type.csv"))

        
    all_type_cnt = pd.concat([db_dict[db_name]['sql_type_count'] for db_name in db_names], axis=1, keys=db_names)
    all_type_cnt.fillna(0, inplace=True)
    all_type_cnt['arithmetic_mean'] = all_type_cnt.mean(axis=1)
    type_cnt_sorted = all_type_cnt.sort_values(by='arithmetic_mean', ascending=False)
    top_type_cnt = type_cnt_sorted[:20]
    top_type_cnt.to_csv(os.path.join(output, f"all_sql_type.csv"))
    top_type_cnt = top_type_cnt.drop(columns=['arithmetic_mean'])
    top_type_cnt.plot(kind='bar', width=0.8)
    plt.xticks(rotation=45, ha='right')
    plt.xlabel("SQL Type")
    plt.ylabel("Percentage")
    plt.tight_layout()
    plt.savefig(os.path.join(output, f"all_sql_type.svg"))
    
    
    # table the standard percentage
    std_df = pd.DataFrame({'DBMS': db_names, 'Percentage of Standard SQL': standard_percentage_overall, 'Percentage of Standard SQL Files': standard_percentage_perfile})
    std_df.style.to_latex(os.path.join(Table_Dir, f"standard_percentage.tex"), siunitx=True)
    print(std_df)
    
    plt.figure()
    fig, ax = plt.subplots()
    x = np.arange(len(db_names))
    bar_width = 0.35
    # Fill the bars with different patterns
    bar1 = ax.bar(x - bar_width/2, standard_percentage_overall,width=bar_width,  label='Overall Standard Percentage', hatch='//', color='white', edgecolor='black')
    bar2 = ax.bar(x + bar_width/2, standard_percentage_perfile,width=bar_width, label='Fully Standard Files', hatch='+', color='white', edgecolor='black')

    # Fix the x-axes.
    ax.set_xticks(x)
    ax.set_xticklabels(db_names)

    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output, f"standard_percentage.svg"))

def generate_test_case_data_from_cache(input:str = 'data/all_sql_type.csv',output:str = Table_Dir, break_down:bool = True):
    top_type_cnt = pd.read_csv(input, index_col=0)
    top_type_cnt.drop(columns=['arithmetic_mean'], inplace=True)
    print(top_type_cnt.info())
    hatches = ['/', 'o', '\\','*'] #, '+', 'x', 'o', 'O', '.', '*']
    # top_type_cnt[:10].plot(kind='bar', width=0.9, figsize=(12, 5))
    analyzer = testanalyzer.TestCaseAnalyzer()
    standard_cases = analyzer.STANDARD_CASES
    # ax.set_aspect(1)
    
    cols = 15
    labels = top_type_cnt.columns.tolist()
    bar_value = top_type_cnt[:cols].values.T
    bar_num = bar_value.shape[1]
    bar_width = 0.21
    
    '''
    Plot all bars in one figure
    '''
    if not break_down:
        plt.figure(figsize=(12, 5))
        for i, type_cnt in enumerate(bar_value):
            plt.bar(np.arange(bar_num) + bar_width * i, type_cnt, hatch=hatches[i%len(hatches)], label=labels[i], width=bar_width, color='white', edgecolor='black')
        plt.legend()
        plt.xticks(np.arange(bar_num) + bar_width + (len(labels) - 1) % 2 / 2 * bar_width , top_type_cnt[:cols].index.tolist(), rotation=45, ha='right')
        rc('text', usetex=True)
        ax = plt.gca()
        ax.set_yscale('log')
        for label in ax.get_xticklabels():
            if label.get_text() in standard_cases:
                # label.set_bbox(dict(facecolor='none', edgecolor='black', boxstyle='bottom'))
                label.set_weight('bold')
        plt.xlabel("SQL Type")
        plt.ylabel("Percentage")
        plt.tight_layout()
        plt.savefig(os.path.join(output, f"all_sql_type.svg"))
    else:
        fig, (ax1, ax2) = plt.subplots(2, 1,sharex=True, figsize=(12, 5))
        lower_limit = 0
        break_limit = 0.06
        upper_limit = 1
        
        for i, type_cnt in enumerate(bar_value):
            ax1.bar(np.arange(bar_num) + bar_width * i, type_cnt, hatch=hatches[i%len(hatches)], label=labels[i], width=bar_width, color='white', edgecolor='black')
            ax2.bar(np.arange(bar_num) + bar_width * i, type_cnt, hatch=hatches[i%len(hatches)], label=labels[i], width=bar_width, color='white', edgecolor='black')
        plt.xticks(np.arange(bar_num) + bar_width + (len(labels) - 1) % 2 / 2 * bar_width , top_type_cnt[:cols].index.tolist(), rotation=45, ha='right')
        
        ax1.set_ylim(break_limit, upper_limit)
        ax2.set_ylim(lower_limit, break_limit)
        ax1.spines['bottom'].set_visible(False)
        ax2.spines['top'].set_visible(False)
        ax1.xaxis.tick_top()
        ax1.tick_params(labeltop=False)
        ax2.xaxis.tick_bottom()
        
        d = .005
        kwargs = dict(transform=ax1.transAxes, color='k', clip_on=False)
        ax1.plot((-d, +d), (-d, +d), **kwargs)
        ax1.plot((1 - d, 1 + d), (-d, +d), **kwargs)
        kwargs.update(transform=ax2.transAxes)
        ax2.plot((-d, +d), (1 - d, 1 + d), **kwargs)
        ax2.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)
        # ax2.set_yscale('log')
        ax1.legend()
        for label in ax2.get_xticklabels():
            if label.get_text() in standard_cases:
                # label.set_bbox(dict(facecolor='none', edgecolor='black', boxstyle='bottom'))
                label.set_weight('bold')
        plt.xlabel("SQL Type")
        plt.tight_layout()
        plt.savefig(os.path.join(output, f"all_sql_type_breakdown.svg"))

    # xtext = plt.xticks(rotation=45, ha='right')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', "--mode", choices=['length', 'dist', 'dist_cache', 'standard'], default='dist')
    parser.add_argument('-o', "--output", type=str, default=Image_Dir, help="output directory")
    arguments = parser.parse_args()
    output = arguments.output

    if arguments.mode == 'length':
        plot_test_case_length(db_names=['sqlite', 'mysql', 'postgresql', 'duckdb'])
    elif arguments.mode == 'dist':
        generate_test_case_data(db_names=['sqlite', 'postgresql', 'duckdb'], output=output)
    elif arguments.mode == 'dist_cache':
        generate_test_case_data_from_cache(input="data/all_sql_type.csv", output=output, break_down=True)
