#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 15:49:33 2020

@author: techfit
"""
import pandas as pd
import os
import warnings as w
w.filterwarnings('ignore',category=Warning)
os.environ['REPO'] = '/Users/greengodfitness/Desktop/TechFit/ds_project'
os.chdir(os.environ['REPO'])

class ProjectFeeds:
    def __init__(self):
        self.check_extention = ['.csv']
        self.metno_folder = '/Users/greengodfitness/Desktop/TechFit/ds_project/data/metno_data'
        self.met_eireann_folder = '/Users/greengodfitness/Desktop/TechFit/ds_project/data/met_eireann_data'
        self.eirgrid_folder = '/Users/greengodfitness/Desktop/TechFit/ds_project/data/eirgrid_data'
        
    def met_eireann_data(self):
        main_met_eireann_data = pd.DataFrame()
        download_path = ''
        print('----------------------')
        print('Cleaning Historic Data:')
        print('----------------------')
        for filename in os.listdir(self.met_eireann_folder):
            if any(ext in filename for ext in self.check_extention):
                print(filename)
                download_path = (os.path.join(self.met_eireann_folder, filename))
                data = pd.read_csv(download_path)
                column_lst = ['date','rain','temp','dewpt','rhum','msl','wdsp','wddir','dir','windsp','seatp']
                # Select columns from the list
                df = data[data.columns.intersection(column_lst)]
                # Add suffix to the columns
                column_str = filename.replace('.csv','')
                df.columns = ['date']+[str(col) + '_' + column_str for col in df.columns if col != 'date']
                if len(main_met_eireann_data) == 0:
                    main_met_eireann_data = df
                else:
                    main_met_eireann_data = pd.merge(main_met_eireann_data,df,how='inner',left_on=['date'],right_on=['date'])
                # Sort the values by Date Time
                main_met_eireann_data = main_met_eireann_data.sort_values('date', ascending = True)
                # De-duping the rows based on most up to date value
                main_met_eireann_data = main_met_eireann_data.drop_duplicates(subset='date', keep='last')
            else:
                pass
        self.met_eireann_data_file = os.path.join(self.met_eireann_folder,'clean_data','main_met_eireann_data.csv')
        main_met_eireann_data.to_csv(os.path.join(self.met_eireann_folder,'clean_data','main_met_eireann_data.csv'), index = False)
            
    def metno_data(self):
        main_metno_data = pd.DataFrame()
        download_path = ''
        print('----------------------')
        print('Cleaning Forecast Data:')
        print('----------------------')
        for filename in os.listdir(self.metno_folder):
            if any(ext in filename for ext in self.check_extention):
                print(filename)
                download_path = (os.path.join(self.metno_folder, filename))
                data = pd.read_csv(download_path)
                column_lst = ['Date_Time','air_pressure_at_sea_level','air_temperature','dew_point_temperature','relative_humidity',
                              'precipitation_amount','wind_speed','wind_from_direction']
                # Select columns from the list
                df = data[data.columns.intersection(column_lst)]
                # Data formatting
                df['air_pressure_at_sea_level'] = df['air_pressure_at_sea_level'].str.replace('hPa', '')
                df['air_temperature'] = df['air_temperature'].str.replace('celsius', '')
                df['dew_point_temperature'] = df['dew_point_temperature'].str.replace('celsius', '')
                df['relative_humidity'] = df['relative_humidity'].str.replace('%', '')
                df['precipitation_amount'] = df['precipitation_amount'].str.replace('mm', '')
                df['wind_speed'] = df['wind_speed'].str.replace('m/s', '').astype('float')*1.943844
                df['wind_from_direction'] = df['wind_from_direction'].str.replace('degrees', '')                
                df.rename(columns={'Date_Time':'date','air_pressure_at_sea_level':'msl','air_temperature':'temp','dew_point_temperature':'dewpt',
                                   'relative_humidity':'rhum','precipitation_amount':'rain','wind_speed':'wdsp','wind_from_direction':'wddir'}, inplace=True)                
                # Add suffix to the columns
                column_str = filename.replace('.csv','')
                df.columns = ['date']+[str(col) + '_' + column_str for col in df.columns if col != 'date']
                if len(main_metno_data) == 0:
                    main_metno_data = df
                else:
                    main_metno_data = pd.merge(main_metno_data,df,how='inner',left_on=['date'],right_on=['date'])
                # Sort the values by Date Time
                main_metno_data = main_metno_data.sort_values('date', ascending = True)
                # De-duping the rows based on most up to date value
                main_metno_data = main_metno_data.drop_duplicates(subset='date', keep='last')
            else:
                pass
        self.metno_data_file = os.path.join(self.metno_folder,'clean_data','main_metno_data.csv')
        main_metno_data.to_csv(os.path.join(self.metno_folder,'clean_data','main_metno_data.csv'), index = False)
    
    def eirgrid_data(self):
        main_eirgrid_data = pd.DataFrame()
        download_path = ''
        print('---------------------------')
        print('Cleaning System Demand Data:')
        print('---------------------------')
        for filename in os.listdir(self.eirgrid_folder):
            if any(ext in filename for ext in self.check_extention):
                print(filename)
                download_path = (os.path.join(self.eirgrid_folder, filename))
                data = pd.read_csv(download_path)
                column_lst = ['DateTime','IE Demand']
                # Select columns from the list
                df = data[data.columns.intersection(column_lst)]
                # Data formatting
                df.rename(columns={'DateTime':'date','IE Demand':'demand'}, inplace=True)                
                # Convert Quarterly data to hourly
                df['date'] = pd.to_datetime(df['date'],format='%Y-%m-%d %H:%M:%S')
                df_hourly = df.groupby(pd.Grouper(key='date',freq='H')).mean().round(1)
                df_hourly['date'] = df_hourly.index
                df = df_hourly[['date','demand']]
                df.reset_index(drop=True, inplace=True)
                if len(main_eirgrid_data) == 0:
                    main_eirgrid_data = df
                else:
                    main_eirgrid_data = pd.concat([main_eirgrid_data, df], axis=0)
                # Sort the values by Date Time
                main_eirgrid_data = main_eirgrid_data.sort_values('date', ascending = True)
                # De-duping the rows based on most up to date value
                main_eirgrid_data = main_eirgrid_data.drop_duplicates(subset='date', keep='last')
            else:
                pass
        self.eirgrid_data_file = os.path.join(self.eirgrid_folder,'clean_data','main_eirgrid_data.csv')
        main_eirgrid_data.to_csv(os.path.join(self.eirgrid_folder,'clean_data','main_eirgrid_data.csv'), index = False)
        
    def data_mapping(self):
        data1 = pd.read_csv(self.met_eireann_data_file)
        data1 = data1[['date','rain_shannon_clare_airport','temp_shannon_clare_airport','dewpt_shannon_clare_airport','rhum_shannon_clare_airport','msl_shannon_clare_airport','wdsp_shannon_clare_airport',
                  'wddir_shannon_clare_airport','rain_north_west_sligo_markree','temp_north_west_sligo_markree','dewpt_north_west_sligo_markree','rhum_north_west_sligo_markree',
                  'msl_north_west_sligo_markree','rain_north_west_donegal_malin_head','temp_north_west_donegal_malin_head','dewpt_north_west_donegal_malin_head','rhum_north_west_donegal_malin_head',
                  'msl_north_west_donegal_malin_head','wdsp_north_west_donegal_malin_head','wddir_north_west_donegal_malin_head','rain_south_west_cork_airport','temp_south_west_cork_airport',
                  'dewpt_south_west_cork_airport','rhum_south_west_cork_airport','msl_south_west_cork_airport','wdsp_south_west_cork_airport','wddir_south_west_cork_airport',
                  'rain_north_west_cavan_ballyhaise','temp_north_west_cavan_ballyhaise','dewpt_north_west_cavan_ballyhaise','rhum_north_west_cavan_ballyhaise','msl_north_west_cavan_ballyhaise',
                  'wdsp_north_west_cavan_ballyhaise','wddir_north_west_cavan_ballyhaise','rain_west_mayo_newport','temp_west_mayo_newport','dewpt_west_mayo_newport','rhum_west_mayo_newport',
                  'msl_west_mayo_newport','wdsp_west_mayo_newport','wddir_west_mayo_newport','rain_west_galway_athenry','temp_west_galway_athenry','dewpt_west_galway_athenry',
                  'rhum_west_galway_athenry','msl_west_galway_athenry','wdsp_west_galway_athenry','wddir_west_galway_athenry','rain_dublin_casement','temp_dublin_casement','dewpt_dublin_casement','rhum_dublin_casement',
                  'msl_dublin_casement','wdsp_dublin_casement','wddir_dublin_casement','rain_south_west_cork_roches_point','temp_south_west_cork_roches_point','dewpt_south_west_cork_roches_point',
                  'rhum_south_west_cork_roches_point','msl_south_west_cork_roches_point','wdsp_south_west_cork_roches_point','wddir_south_west_cork_roches_point','rain_shannon_tipperary_gurteen',
                  'temp_shannon_tipperary_gurteen','dewpt_shannon_tipperary_gurteen','rhum_shannon_tipperary_gurteen','msl_shannon_tipperary_gurteen','wdsp_shannon_tipperary_gurteen',
                  'wddir_shannon_tipperary_gurteen','rain_midlands_meath_dunsany','temp_midlands_meath_dunsany','dewpt_midlands_meath_dunsany','rhum_midlands_meath_dunsany','msl_midlands_meath_dunsany',
                  'wdsp_midlands_meath_dunsany','wddir_midlands_meath_dunsany','rain_south_west_kerry_valentia_observatory','temp_south_west_kerry_valentia_observatory',
                  'dewpt_south_west_kerry_valentia_observatory','rhum_south_west_kerry_valentia_observatory','msl_south_west_kerry_valentia_observatory','wdsp_south_west_kerry_valentia_observatory',
                  'wddir_south_west_kerry_valentia_observatory','rain_midlands_carlow_oak_park','temp_midlands_carlow_oak_park','dewpt_midlands_carlow_oak_park','rhum_midlands_carlow_oak_park',
                  'msl_midlands_carlow_oak_park','wdsp_midlands_carlow_oak_park','wddir_midlands_carlow_oak_park','rain_west_galway_mace_head','temp_west_galway_mace_head','dewpt_west_galway_mace_head',
                  'rhum_west_galway_mace_head','msl_west_galway_mace_head','wdsp_west_galway_mace_head','wddir_west_galway_mace_head','rain_dublin_airport','temp_dublin_airport','dewpt_dublin_airport',
                  'rhum_dublin_airport','msl_dublin_airport','wdsp_dublin_airport','wddir_dublin_airport','rain_shannon_fermoy_moore_park','temp_shannon_fermoy_moore_park',
                  'dewpt_shannon_fermoy_moore_park','rhum_shannon_fermoy_moore_park','msl_shannon_fermoy_moore_park','wdsp_shannon_fermoy_moore_park','wddir_shannon_fermoy_moore_park',
                  'rain_west_mayo_belmullet','temp_west_mayo_belmullet','dewpt_west_mayo_belmullet','rhum_west_mayo_belmullet','msl_west_mayo_belmullet','wdsp_west_mayo_belmullet',
                  'wddir_west_mayo_belmullet','rain_north_west_donegal_finner','temp_north_west_donegal_finner','dewpt_north_west_donegal_finner','rhum_north_west_donegal_finner',
                  'msl_north_west_donegal_finner','wdsp_north_west_donegal_finner','wddir_north_west_donegal_finner','rain_midlands_westmeath_mullingar','temp_midlands_westmeath_mullingar',
                  'dewpt_midlands_westmeath_mullingar','rhum_midlands_westmeath_mullingar','msl_midlands_westmeath_mullingar','wdsp_midlands_westmeath_mullingar','wddir_midlands_westmeath_mullingar',
                  'rain_west_mayo_knock_airport','temp_west_mayo_knock_airport','dewpt_west_mayo_knock_airport','rhum_west_mayo_knock_airport','msl_west_mayo_knock_airport',
                  'wdsp_west_mayo_knock_airport','wddir_west_mayo_knock_airport','rain_dublin_phoenix_park','temp_dublin_phoenix_park','dewpt_dublin_phoenix_park','rhum_dublin_phoenix_park',
                  'msl_dublin_phoenix_park','rain_south_east_wexford_johnstownii','temp_south_east_wexford_johnstownii','dewpt_south_east_wexford_johnstownii','rhum_south_east_wexford_johnstownii',
                  'msl_south_east_wexford_johnstownii','wdsp_south_east_wexford_johnstownii','wddir_south_east_wexford_johnstownii','rain_west_mayo_claremorris','temp_west_mayo_claremorris',
                  'dewpt_west_mayo_claremorris','rhum_west_mayo_claremorris','msl_west_mayo_claremorris','wdsp_west_mayo_claremorris','wddir_west_mayo_claremorris',
                  'rain_north_west_roscommon_mt_dillon','temp_north_west_roscommon_mt_dillon','dewpt_north_west_roscommon_mt_dillon','rhum_north_west_roscommon_mt_dillon',
                  'msl_north_west_roscommon_mt_dillon','wdsp_north_west_roscommon_mt_dillon','wddir_north_west_roscommon_mt_dillon','rain_south_west_cork_sherkinIsland',
                  'temp_south_west_cork_sherkinIsland','dewpt_south_west_cork_sherkinIsland','rhum_south_west_cork_sherkinIsland','msl_south_west_cork_sherkinIsland',
                  'wdsp_south_west_cork_sherkinIsland','wddir_south_west_cork_sherkinIsland']]
        data1 = data1.loc[:,:].round(1)
        data1['date'] = pd.to_datetime(data1['date'],format='%d-%b-%Y %H:%M')
        data2 = pd.read_csv(self.metno_data_file)
        data2 = data2[['date','rain_shannon_clare_airport','temp_shannon_clare_airport','dewpt_shannon_clare_airport','rhum_shannon_clare_airport','msl_shannon_clare_airport','wdsp_shannon_clare_airport',
                  'wddir_shannon_clare_airport','rain_north_west_sligo_markree','temp_north_west_sligo_markree','dewpt_north_west_sligo_markree','rhum_north_west_sligo_markree',
                  'msl_north_west_sligo_markree','rain_north_west_donegal_malin_head','temp_north_west_donegal_malin_head','dewpt_north_west_donegal_malin_head','rhum_north_west_donegal_malin_head',
                  'msl_north_west_donegal_malin_head','wdsp_north_west_donegal_malin_head','wddir_north_west_donegal_malin_head','rain_south_west_cork_airport','temp_south_west_cork_airport',
                  'dewpt_south_west_cork_airport','rhum_south_west_cork_airport','msl_south_west_cork_airport','wdsp_south_west_cork_airport','wddir_south_west_cork_airport',
                  'rain_north_west_cavan_ballyhaise','temp_north_west_cavan_ballyhaise','dewpt_north_west_cavan_ballyhaise','rhum_north_west_cavan_ballyhaise','msl_north_west_cavan_ballyhaise',
                  'wdsp_north_west_cavan_ballyhaise','wddir_north_west_cavan_ballyhaise','rain_west_mayo_newport','temp_west_mayo_newport','dewpt_west_mayo_newport','rhum_west_mayo_newport',
                  'msl_west_mayo_newport','wdsp_west_mayo_newport','wddir_west_mayo_newport','rain_west_galway_athenry','temp_west_galway_athenry','dewpt_west_galway_athenry',
                  'rhum_west_galway_athenry','msl_west_galway_athenry','wdsp_west_galway_athenry','wddir_west_galway_athenry','rain_dublin_casement','temp_dublin_casement','dewpt_dublin_casement','rhum_dublin_casement',
                  'msl_dublin_casement','wdsp_dublin_casement','wddir_dublin_casement','rain_south_west_cork_roches_point','temp_south_west_cork_roches_point','dewpt_south_west_cork_roches_point',
                  'rhum_south_west_cork_roches_point','msl_south_west_cork_roches_point','wdsp_south_west_cork_roches_point','wddir_south_west_cork_roches_point','rain_shannon_tipperary_gurteen',
                  'temp_shannon_tipperary_gurteen','dewpt_shannon_tipperary_gurteen','rhum_shannon_tipperary_gurteen','msl_shannon_tipperary_gurteen','wdsp_shannon_tipperary_gurteen',
                  'wddir_shannon_tipperary_gurteen','rain_midlands_meath_dunsany','temp_midlands_meath_dunsany','dewpt_midlands_meath_dunsany','rhum_midlands_meath_dunsany','msl_midlands_meath_dunsany',
                  'wdsp_midlands_meath_dunsany','wddir_midlands_meath_dunsany','rain_south_west_kerry_valentia_observatory','temp_south_west_kerry_valentia_observatory',
                  'dewpt_south_west_kerry_valentia_observatory','rhum_south_west_kerry_valentia_observatory','msl_south_west_kerry_valentia_observatory','wdsp_south_west_kerry_valentia_observatory',
                  'wddir_south_west_kerry_valentia_observatory','rain_midlands_carlow_oak_park','temp_midlands_carlow_oak_park','dewpt_midlands_carlow_oak_park','rhum_midlands_carlow_oak_park',
                  'msl_midlands_carlow_oak_park','wdsp_midlands_carlow_oak_park','wddir_midlands_carlow_oak_park','rain_west_galway_mace_head','temp_west_galway_mace_head','dewpt_west_galway_mace_head',
                  'rhum_west_galway_mace_head','msl_west_galway_mace_head','wdsp_west_galway_mace_head','wddir_west_galway_mace_head','rain_dublin_airport','temp_dublin_airport','dewpt_dublin_airport',
                  'rhum_dublin_airport','msl_dublin_airport','wdsp_dublin_airport','wddir_dublin_airport','rain_shannon_fermoy_moore_park','temp_shannon_fermoy_moore_park',
                  'dewpt_shannon_fermoy_moore_park','rhum_shannon_fermoy_moore_park','msl_shannon_fermoy_moore_park','wdsp_shannon_fermoy_moore_park','wddir_shannon_fermoy_moore_park',
                  'rain_west_mayo_belmullet','temp_west_mayo_belmullet','dewpt_west_mayo_belmullet','rhum_west_mayo_belmullet','msl_west_mayo_belmullet','wdsp_west_mayo_belmullet',
                  'wddir_west_mayo_belmullet','rain_north_west_donegal_finner','temp_north_west_donegal_finner','dewpt_north_west_donegal_finner','rhum_north_west_donegal_finner',
                  'msl_north_west_donegal_finner','wdsp_north_west_donegal_finner','wddir_north_west_donegal_finner','rain_midlands_westmeath_mullingar','temp_midlands_westmeath_mullingar',
                  'dewpt_midlands_westmeath_mullingar','rhum_midlands_westmeath_mullingar','msl_midlands_westmeath_mullingar','wdsp_midlands_westmeath_mullingar','wddir_midlands_westmeath_mullingar',
                  'rain_west_mayo_knock_airport','temp_west_mayo_knock_airport','dewpt_west_mayo_knock_airport','rhum_west_mayo_knock_airport','msl_west_mayo_knock_airport',
                  'wdsp_west_mayo_knock_airport','wddir_west_mayo_knock_airport','rain_dublin_phoenix_park','temp_dublin_phoenix_park','dewpt_dublin_phoenix_park','rhum_dublin_phoenix_park',
                  'msl_dublin_phoenix_park','rain_south_east_wexford_johnstownii','temp_south_east_wexford_johnstownii','dewpt_south_east_wexford_johnstownii','rhum_south_east_wexford_johnstownii',
                  'msl_south_east_wexford_johnstownii','wdsp_south_east_wexford_johnstownii','wddir_south_east_wexford_johnstownii','rain_west_mayo_claremorris','temp_west_mayo_claremorris',
                  'dewpt_west_mayo_claremorris','rhum_west_mayo_claremorris','msl_west_mayo_claremorris','wdsp_west_mayo_claremorris','wddir_west_mayo_claremorris',
                  'rain_north_west_roscommon_mt_dillon','temp_north_west_roscommon_mt_dillon','dewpt_north_west_roscommon_mt_dillon','rhum_north_west_roscommon_mt_dillon',
                  'msl_north_west_roscommon_mt_dillon','wdsp_north_west_roscommon_mt_dillon','wddir_north_west_roscommon_mt_dillon','rain_south_west_cork_sherkinIsland',
                  'temp_south_west_cork_sherkinIsland','dewpt_south_west_cork_sherkinIsland','rhum_south_west_cork_sherkinIsland','msl_south_west_cork_sherkinIsland',
                  'wdsp_south_west_cork_sherkinIsland','wddir_south_west_cork_sherkinIsland']]
        data2 = data2.loc[:,:].round(1)
        data2['date'] = pd.to_datetime(data2['date'],format='%Y-%m-%d %H:%M:%S')
        weather_data = pd.concat([data1, data2], axis=0)
        weather_data['date'] = pd.to_datetime(weather_data['date'],format='%d/%m/%Y %H:%M')
        weather_data = weather_data.sort_values('date', ascending = True)
        weather_data.to_csv(os.path.join('data','weather_data.csv'), index = False)
        self.weather_data = weather_data
        data3 = pd.read_csv(self.eirgrid_data_file)
        data3['date'] = pd.to_datetime(data3['date'],format='%Y-%m-%d %H:%M:%S')
        # Create final dataset
        project_data = pd.merge(weather_data,data3,how='inner',left_on=['date'],right_on=['date'])
        # Convert objects into floats
        project_data_int = project_data.select_dtypes(['int64'])
        project_data_object = project_data.select_dtypes(['object'])
        project_data[project_data_int.columns] = project_data_int.apply(lambda x: x.astype(float))
        project_data[project_data_object.columns] = project_data_object.replace(' ','').replace(' ','').apply(pd.to_numeric).apply(lambda x: x.astype(float))
        project_data.to_csv(os.path.join('data','project_data.csv'), index = False)
        self.project_data = project_data
        
if __name__ == '__main__':
    feeds = ProjectFeeds()
    feeds.met_eireann_data()
    feeds.metno_data()
    feeds.eirgrid_data()
    feeds.data_mapping()
    project_data = feeds.project_data
    