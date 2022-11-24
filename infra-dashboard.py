#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import scipy as sp
import numpy as np
from os import walk, stat
from hd_var import *
from functools import partial


# In[2]:


path = 'S:\\ISR\\Branch - Investment Risk\\Risk Oversight\\Working\\Minghao\\IRR-dashboard-automation'
sector_df =  pd.read_excel(
    f'{path}\\sector-mapping.xlsx'
) 
sector_dict = sector_df.set_index('Name').to_dict()['Sector']
unique_sect = set(sector_df['Sector'])

direct_sectors = [
    'Electric & Gas Utilities',
    'Agriculture',
    'Telecommunication',
    'Water & WW Utilities',
    'Transportation',
    'Timber',
    'Multi-Utilities',
    'Renewable Power',
    'Other',
    'Total'
]
fund_sectors = [
    'Agriculture (Funds)',
    'Energy',
    'General Infrastructure',
    'Timber (Funds)',
    'Total'
]

fund_sector_dict = sector_df.set_index('Fund').to_dict()['Sector']


# In[3]:


# generate a list of csv files for the script to read from

tree = [i for i in walk(path)]
lst = []

for i in range(1, len(tree)):
    for j, item in enumerate(tree[i][2]):
        file = f'{tree[i][0]}\\{item}'
        # set a lower limit of file size to filter out the settings & summary reports
        if stat(file).st_size > 5000:
            lst.append(file)
            
df_names = [i.split('\\')[1] for i in lst]
# print to verify which RM reports we are looking at based on sub-director names
print(df_names)

# "dfs" is a list that stores all dataframes created using the csv files
# the order in the list corresponds to the order of df_names
dfs = []
for i, x in enumerate(lst):
    dfs.append(pd.read_csv(lst[i], thousands=','))


# In[4]:


# clean up dataframes' column labels to make them more readable

col_names = [[] for _ in range(len(dfs))]
for i, df in enumerate(dfs):
    temp = df.columns.to_list()
    col_names[i].append(temp[0]) # append 'level' since it doesn't have brackets
    for j in temp[1:]:
        label_short = j[j.rfind("[") + 1:j.rfind("]")]
        col_names[i].append(label_short)
        
for i, df in enumerate(dfs):
    df.columns = col_names[i]
    df.drop('level', inplace = True, axis = 1)


# In[5]:


# create four DataFrames:
# fund_pv_df, fund_pnl_df
# direct_pv_df, direct_pnl_df

fund_pnl_df = dfs[1].filter(
    items = [col for col in dfs[1].columns if 'Fund' in col]
)

direct_pnl_df = dfs[1].filter(
    items = [col for col in dfs[1].columns if 'Direct' in col]
)

fund_pv_df = dfs[2].loc[
    dfs[2]['acInvestmentType']=='Fund', ['fundName','PV']
].groupby('fundName').sum()

direct_pv_df = dfs[2].loc[
    dfs[2]['acInvestmentType']=='Direct', ['fundName','PV']
].groupby('fundName').sum()

scenario = dfs[0]['Scenario'].to_list()

# re-construct DataFrame index

for df in [fund_pnl_df, direct_pnl_df]:
    if 'Scenario' in df.columns:
        df.drop('Scenario', axis=1, inplace=True)
    df.columns = [i.split('\\')[-1] for i in df.columns]
    df.index = scenario


# In[6]:


# assign constant PV numbers

tot_pv = dfs[2].loc[0,'PV']
fund_pv = fund_pv_df['PV'].sum()
direct_pv = direct_pv_df['PV'].sum()

# Total IRR PnL as a list to pass into percentage VaR calculations

tot_pnl = dfs[0]['PNL\Total\Total'].to_list()


# In[7]:


# from pyinstrument import Profiler
# profiler = Profiler()
# profiler.start()

total_result = pd.DataFrame(
    {
        'Name': ['I&RR Total'],
        'PV': [tot_pv],
        'VaR': [hd_var_ann(tot_pnl, tot_pv)],
        'Risk Contribution to I&RR': [1]
    }
)

for inv_type in ['fund', 'direct']:
    sectors = vars()[f'{inv_type}_sectors']
    
    pv = []
    var = []
    contrib = []
    var_lst =[]
    
    pnl_df = vars()[f'{inv_type}_pnl_df']
    pv_df = vars()[f'{inv_type}_pv_df']
    
    pv_lst = [pv_df['PV'].sum()] + pv_df['PV'].to_list()
    
    var_lst = [hd_var_ann(pnl_df.iloc[:, i], x, ci=0.95, factor=25.2**0.5) 
               for i, x in enumerate(pv_lst)]
    contrib_lst = [hd_contrib(tot_pnl, pnl_df.iloc[:, i]) 
                   for i in range(len(pv_lst))]
    
    var_df = pd.DataFrame(
        {
        'Name': list(pnl_df),
        'PV': pv_lst,
        'VaR': var_lst,
        'Risk Contribution to I&RR': contrib_lst
        }
    )
    
    vars()[f'{inv_type}_var_df'] = var_df.sort_values(by = 'VaR', axis=0, ascending = False)
    vars()[f'{inv_type}_var_df'].reset_index(drop = True, inplace = True)
    
    for i, sect in enumerate(sectors):      
        # iterates through all sectors in Funds
        if sect == 'Total':
            assets = ['Total']
            sector_pv = vars()[f'{inv_type}_pv']
        else:
            assets = [s for s in list(pnl_df) if fund_sector_dict.get(s) == sect]
            # list of PNL columns names that fall into the current sector
            df = dfs[2]
            sector_pv = df[df['fundName'].isin(assets)]['PV'].sum(axis=0)
        # print(f'{sect}: {funds_in_sect}')
        # print(funds_in_sect)
        pnl = pnl_df[assets].sum(axis = 1) # this is a PnL list for a certain sector
        pv.append(sector_pv)
        var.append(hd_var_ann(pnl, sector_pv))
        contrib.append(hd_contrib(tot_pnl, pnl))
    
    result = pd.DataFrame({'Sub-Industry': sectors, 'PV': pv, 'VaR': var, 'Risk Contribution to I&RR': contrib})
    vars()[f'{inv_type}_result'] = result
    
# profiler.stop()
# profiler.print()


# In[8]:


writer = pd.ExcelWriter(f'{path}\\irr_calculation_results.xlsx')
for df in ['total_result', 'fund_result', 'direct_result', 'fund_var_df', 'direct_var_df']:
    vars()[df].to_excel(writer, sheet_name = df)
writer.save()


# In[ ]:




