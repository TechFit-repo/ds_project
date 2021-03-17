#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 15:49:33 2020

@author: techfit
"""
from metno_locationforecast import Place, Forecast
import pandas as pd
import requests
import zipfile
import json
import warnings as w
w.filterwarnings('ignore',category=Warning)
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep
import os
os.environ['REPO'] = '/Users/greengodfitness/Desktop/TechFit/ds_project'
os.chdir(os.environ['REPO'])
met_eireann_config = json.loads(open('src/met_eireann_config.json').read())
 
class WeatherDataFeed:
    '''
    Loading Current Weather Data from Met.no API

    Parameters
    ----------
    links : Locations
    symbol - Symbol of the Stock to extract the data
    
    Returns
    -------
    Integer
    DataFrame

    '''
    def __init__(self):
        self.USER_AGENT = 'metno_locationforecast/1.0'
        self.check_extention = ['.csv','.xlsx']
        self.sourceFolder = '/Users/greengodfitness/Desktop/TechFit/ds_project/data/metno_data'
        self.downloadFolder = '/Users/greengodfitness/Downloads/'
        self.met_eireann_zip_folder = '/Users/greengodfitness/Desktop/TechFit/ds_project/data/met_eireann_data_zip'
        self.eirgrid_folder = '/Users/greengodfitness/Desktop/TechFit/ds_project/data/eirgrid_data'
        self.link='https://www.eirgridgroup.com/'
        self.files = ['2020/21','2018/19','2016/17','2014/15']
        
    def get_weather_data(self, county='Dublin', latitude=53.33, longitude=-6.24, altitude=10, save_location='./data/data'):
        self.county = Place(county, latitude, longitude, altitude)
        self.forecast = Forecast(self.county, self.USER_AGENT, 'complete', save_location)
        self.forecast.update()
        df = pd.DataFrame()
        for interval in self.forecast.data.intervals:
            row = pd.DataFrame.from_dict(interval.variables,orient='index')
            row['start_time'] = interval.start_time
            row['end_time'] = interval.end_time
            df = df.append(row)
            
        df.columns = ['weather_feeds', 'start_time', 'end_time']
        df['weather_feeds'] = df['weather_feeds'].apply(str)
        df[['header','value']] = df.weather_feeds.str.split(expand=True)
        df['header'] = df['header'].str.replace(':', '')
        df2 = df.pivot(index = 'start_time', columns = 'header', values='value')
        self.df2 = df2
    
    def save_weather_data(self, file_name = 'default.csv'):
        weather_data = self.df2
        weather_data['Date_Time'] = weather_data.index
        weather_data = weather_data[['Date_Time',
                                   'air_pressure_at_sea_level',
                                   'air_temperature',
                                   'air_temperature_max',
                                   'air_temperature_min',
                                   'cloud_area_fraction',
                                   'cloud_area_fraction_high',
                                   'cloud_area_fraction_low',
                                   'cloud_area_fraction_medium',
                                   'dew_point_temperature',
                                   'fog_area_fraction',
                                   'precipitation_amount',
                                   'relative_humidity',
                                   'ultraviolet_index_clear_sky',
                                   'wind_from_direction',
                                   'wind_speed']]
        # Run First time like this to include headers and second time you can use append mode and not include headers
        weather_data.to_csv(os.path.join(os.getcwd(),'data/metno_data',file_name), index = False)
        # weather_data.to_csv(os.path.join(os.getcwd(),file_name), mode='a', header=False, index = False)  
        self.file_name = file_name
    
    def clean_up_weather_data(self):
        download_path = ''
        for filename in os.listdir(self.sourceFolder):
            if any(ext in filename for ext in self.check_extention):
                if filename == self.file_name:
                    print(filename)
                    download_path = (os.path.join(self.sourceFolder,filename))
                    data = pd.read_csv(download_path)
                    df = pd.DataFrame(data)
                    # Sort the values by Date Time
                    df = df.sort_values('Date_Time', ascending = True)
                    # De-duping the rows based on most up to date value
                    df = df.drop_duplicates(subset='Date_Time', keep='last')
                    df.to_csv(download_path, index = False)
                else:
                    pass
            else:
                pass

    def get_historical_weather_data(self, url='https://cli.fusio.net/cli/climate_data/webdata/hly775.zip', file_name='south_west_cork_sherkinIsland.csv'):
        # download the file contents in binary format
        r = requests.get(url)
        # open method to open a file on your system and write the contents
        with open(os.path.join(self.met_eireann_zip_folder,url.replace('https://cli.fusio.net/cli/climate_data/webdata/','')), 'wb') as code:
            code.write(r.content)
        unzipped_file = zipfile.ZipFile(os.path.join(self.met_eireann_zip_folder,url.replace('https://cli.fusio.net/cli/climate_data/webdata/','')), 'r')
        filename = url.replace('https://cli.fusio.net/cli/climate_data/webdata/','')        
        list_15 = ['dublin_phoenix_park.csv','north_west_sligo_markree.csv']
        list_17 = ['south_west_cork_sherkinIsland.csv','south_west_cork_roches_point.csv',
                   'shannon_fermoy_moore_park.csv','south_east_wexford_johnstownii.csv','midlands_carlow_oak_park.csv','shannon_tipperary_gurteen.csv',
                   'west_galway_mace_head.csv','west_mayo_newport.csv','west_mayo_claremorris.csv',
                   'west_galway_athenry.csv','north_west_roscommon_mt_dillon.csv','midlands_westmeath_mullingar.csv','midlands_meath_dunsany.csv',
                   'north_west_cavan_ballyhaise.csv','north_west_donegal_finner.csv']
        list_23 = ['south_west_kerry_valentia_observatory.csv','south_west_cork_airport.csv','shannon_clare_airport.csv','west_mayo_knock_airport.csv','dublin_airport.csv',
                   'west_mayo_belmullet.csv','dublin_casement.csv','north_west_donegal_malin_head.csv']
        if file_name in list_15:
            file = pd.read_csv(unzipped_file.open(filename.replace('.zip','.csv')), skiprows = 15)
        elif file_name in list_17:
            file = pd.read_csv(unzipped_file.open(filename.replace('.zip','.csv')), skiprows = 17)
        elif file_name in list_23:
            file = pd.read_csv(unzipped_file.open(filename.replace('.zip','.csv')), skiprows = 23)
        else:
            print('Invalid header location..')
        file.to_csv(os.path.join(os.getcwd(),'data/met_eireann_data',file_name), index = False)
        print(file_name + ' data is updated')
        
    def get_system_demand_data(self):
        driver = webdriver.Chrome(ChromeDriverManager().install())
        driver.set_window_size(1120, 1000)
        driver.get(self.link)
        sleep(4)
        try:
            driver.find_element_by_xpath("//button[contains(text(), 'Accept all')]")\
                .click()
            driver.get("https://www.eirgridgroup.com/how-the-grid-works/renewables/")
            driver.find_element_by_xpath("//a[contains(text(), 'System and Renewable Data ')]")\
                        .click()
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            for i in self.files:
                sleep(7)    
                str1 = str('//a[contains(text(), "')
                str2 = str(i)
                str3 = str('")]')
                str_path = str1+str2+str3
                driver.find_element_by_xpath(str_path)\
                        .click()
        except:
            sleep(2)
            print('Connection failed...')
        sleep(6)
        download_path = ''
        for filename in os.listdir(self.downloadFolder):
            if any(ext in filename for ext in self.check_extention):
                print(filename)
                download_path = (os.path.join(self.downloadFolder, filename))
                df = pd.read_excel(download_path, index_col=None, na_values=['NA'], usecols = "A,A:K")
                upload_path = filename.replace('.xlsx','.csv')
                df.to_csv(os.path.join(self.eirgrid_folder,upload_path), index = False)
            else:
                pass
        sleep(6)
        driver.close()
            
if __name__ == '__main__':
    weather = WeatherDataFeed()
    for i in met_eireann_config['dict']:
            key=i
            value=met_eireann_config['dict'][i]
            weather.get_historical_weather_data(url=key,file_name=value)
    weather.get_system_demand_data()
    weather.get_weather_data(county='Dublin Airport', latitude=53.43, longitude=-6.24, altitude=10)
    weather.save_weather_data(file_name = 'dublin_airport.csv')
    weather.clean_up_weather_data()
    print('Dublin Airport data is updated')
    
    weather = WeatherDataFeed()
    weather.get_weather_data(county='Dublin', latitude=53.30, longitude=-6.44, altitude=10)
    weather.save_weather_data(file_name = 'dublin_casement.csv')
    weather.clean_up_weather_data()
    print('Dublin Casement data is updated')
    
    weather = WeatherDataFeed()
    weather.get_weather_data(county='Dublin', latitude=53.36, longitude=-6.33, altitude=10)
    weather.save_weather_data(file_name = 'dublin_phoenix_park.csv')
    weather.clean_up_weather_data()
    print('Dublin Phoenix Park data is updated')
    
    weather.get_weather_data(county='Carlow', latitude=52.84, longitude=-6.92, altitude=10)
    weather.save_weather_data(file_name = 'midlands_carlow_oak_park.csv')
    weather.clean_up_weather_data()
    print('Carlow Oak Park data is updated')
    db = weather.df2
    
    weather.get_weather_data(county='Meath', latitude=53.54, longitude=-6.62, altitude=10)
    weather.save_weather_data(file_name = 'midlands_meath_dunsany.csv')
    weather.clean_up_weather_data()
    print('Meath Dunsany data is updated')
    db = weather.df2
    
    weather.get_weather_data(county='Westmeath', latitude=53.53, longitude=-7.34, altitude=10)
    weather.save_weather_data(file_name = 'midlands_westmeath_mullingar.csv')
    weather.clean_up_weather_data()
    print('Westmeath Mullingar data is updated')
    db = weather.df2
    
    weather.get_weather_data(county='Cavan', latitude=54.04, longitude=-7.32, altitude=10)
    weather.save_weather_data(file_name = 'north_west_cavan_ballyhaise.csv')
    weather.clean_up_weather_data()
    print('Cavan Ballyhaise data is updated')
    db = weather.df2
    
    weather.get_weather_data(county='Donegal', latitude=54.49, longitude=-8.27, altitude=10)
    weather.save_weather_data(file_name = 'north_west_donegal_finner.csv')
    weather.clean_up_weather_data()
    print('Donegal Finner data is updated')
    db = weather.df2
    
    weather.get_weather_data(county='Donegal', latitude=55.37, longitude=-7.40, altitude=10)
    weather.save_weather_data(file_name = 'north_west_donegal_malin_head.csv')
    weather.clean_up_weather_data()
    print('Donegal Malin Head data is updated')
    db = weather.df2
    
    weather.get_weather_data(county='Roscommon', latitude=53.73, longitude=-8.05, altitude=10)
    weather.save_weather_data(file_name = 'north_west_roscommon_mt_dillon.csv')
    weather.clean_up_weather_data()
    print('Roscommon MT Dillon data is updated')
    db = weather.df2
    
    weather.get_weather_data(county='Roscommon', latitude=53.73, longitude=-8.05, altitude=10)
    weather.save_weather_data(file_name = 'north_west_roscommon_mt_dillon.csv')
    weather.clean_up_weather_data()
    print('Roscommon MT Dillon data is updated')
    db = weather.df2
    
    weather.get_weather_data(county='Sligo', latitude=54.28, longitude=-8.48, altitude=10)
    weather.save_weather_data(file_name = 'north_west_sligo_markree.csv')
    weather.clean_up_weather_data()
    print('Sligo Markee data is updated')
    db = weather.df2
    
    weather.get_weather_data(county='Shannon Airport', latitude=52.70, longitude=-8.92, altitude=10)
    weather.save_weather_data(file_name = 'shannon_clare_airport.csv')
    weather.clean_up_weather_data()
    print('Shannon Clare Airport data is updated')
    db = weather.df2
    
    weather.get_weather_data(county='Cork', latitude=52.17, longitude=-8.24, altitude=10)
    weather.save_weather_data(file_name = 'shannon_fermoy_moore_park.csv')
    weather.clean_up_weather_data()
    print('Shannon Fermoy Moorepark data is updated')
    db = weather.df2
    
    weather.get_weather_data(county='Shannon', latitude=53.12, longitude=-8.01, altitude=10)
    weather.save_weather_data(file_name = 'shannon_tipperary_gurteen.csv')
    weather.clean_up_weather_data()
    print('Shannon Tipperary Gurteen data is updated')
    db = weather.df2
    
    weather.get_weather_data(county='Wexford', latitude=52.34, longitude=-6.46, altitude=10)
    weather.save_weather_data(file_name = 'south_east_wexford_johnstownii.csv')
    weather.clean_up_weather_data()
    print('South East Wexford Johnstownii data is updated')
    db = weather.df2
    
    weather.get_weather_data(county='Cork Airport', latitude=51.84, longitude=-8.49, altitude=10)
    weather.save_weather_data(file_name = 'south_west_cork_airport.csv')
    weather.clean_up_weather_data()
    print('South West Cork Airport data is updated')
    db = weather.df2
    
    weather.get_weather_data(county='Cork', latitude=51.79, longitude=-8.25, altitude=10)
    weather.save_weather_data(file_name = 'south_west_cork_roches_point.csv')
    weather.clean_up_weather_data()
    print('South West Cork Roches Point data is updated')
    db = weather.df2
    
    weather.get_weather_data(county='Cork', latitude=51.47, longitude=-9.42, altitude=10)
    weather.save_weather_data(file_name = 'south_west_cork_sherkinIsland.csv')
    weather.clean_up_weather_data()
    print('South West Cork SherkinIsland data is updated')
    db = weather.df2
    
    weather = WeatherDataFeed()
    weather.get_weather_data(county='Kerry', latitude=51.93, longitude=-10.24, altitude=10)
    weather.save_weather_data(file_name = 'south_west_kerry_valentia_observatory.csv')
    weather.clean_up_weather_data()
    print('South West Kerry Valentia Observatory data is updated')
    
    weather.get_weather_data(county='Galway', latitude=53.30, longitude=-8.75, altitude=10)
    weather.save_weather_data(file_name = 'west_galway_athenry.csv')
    weather.clean_up_weather_data()
    print('West Galway Atherny Airport data is updated')
    db = weather.df2
    
    weather = WeatherDataFeed()
    weather.get_weather_data(county='Galway', latitude=53.32, longitude=-9.90, altitude=10)
    weather.save_weather_data(file_name = 'west_galway_mace_head.csv')
    weather.clean_up_weather_data()
    print('West Galway Mace Head data is updated')
    
    weather = WeatherDataFeed()
    weather.get_weather_data(county='Mayo', latitude=54.21, longitude=-9.98, altitude=10)
    weather.save_weather_data(file_name = 'west_mayo_belmullet.csv')
    weather.clean_up_weather_data()
    print('West Mayo Belmullet data is updated')
    
    weather = WeatherDataFeed()
    weather.get_weather_data(county='Mayo', latitude=53.71, longitude=-8.99, altitude=10)
    weather.save_weather_data(file_name = 'west_mayo_claremorris.csv')
    weather.clean_up_weather_data()
    print('West Mayo Claremorris data is updated')
    
    weather = WeatherDataFeed()
    weather.get_weather_data(county='Mayo', latitude=53.91, longitude=-8.81, altitude=10)
    weather.save_weather_data(file_name = 'west_mayo_knock_airport.csv')
    weather.clean_up_weather_data()
    print('West Mayo Knock Airport data is updated')
    
    weather = WeatherDataFeed()
    weather.get_weather_data(county='Mayo', latitude=53.88, longitude=-9.55, altitude=10)
    weather.save_weather_data(file_name = 'west_mayo_newport.csv')
    weather.clean_up_weather_data()
    print('West Mayo Newport data is updated')
