#!/usr/bin/env python3

import argparse
import datetime
import requests
import sys
import time

url_base='https://monitoringapi.solaredge.com'

def _string_to_datetime(date):
  return datetime.datetime.fromtimestamp(time.mktime(time.strptime(date, '%Y-%m-%d')))

def getSiteDates(site, key):
  resp = requests.get(url=f'{url_base}/site/{site}/dataPeriod',
    params=dict(
      api_key=key,
    ))
  data = resp.json()

  start_date=data['dataPeriod']['startDate']
  end_date=data['dataPeriod']['endDate']

  return _string_to_datetime(start_date), _string_to_datetime(end_date)

def getSitePowerDetails(site, key, start_date, end_date):
  resp = requests.get(url=f'{url_base}/site/{site}/powerDetails',
    params=dict(
      api_key=key,
      timeUnit='QUARTER_OF_AN_HOUR',
      unit='kw',
      startTime=datetime.date.strftime(start_date, '%Y-%m-%d 00:00:00'),
      endTime=datetime.date.strftime(end_date, '%Y-%m-%d 00:00:00'),
      meters='PRODUCTION,CONSUMPTION,SELFCONSUMPTION,FEEDIN,PURCHASED',
    ))
  return resp.json()

def monthly_it(start_date, end_date):
  yield start_date
  while start_date < end_date:
    start_date += datetime.timedelta(weeks=4)
    yield start_date

def getSiteData(site, key, start_date, end_date):
  for month in monthly_it(start_date, end_date):
    next_month = month + datetime.timedelta(weeks=4)
    if next_month > end_date:
      next_month = end_date
    powerDetails = getSitePowerDetails(site, key, month, next_month)['powerDetails']['meters']
    if month == start_date:
      for meter in powerDetails:
        print(meter['type'], end=',')
      print()

    for index in range (len(powerDetails[0]['values'])):
      print(powerDetails[0]['values'][index]['date'], end=' ')
      for meter in powerDetails:
        print(meter['values'][index].get('value', 0) * 25/1000, end=',')
      print()

def parseCommandLine():
  parser = argparse.ArgumentParser(description='Pull SolarEdge data into CSV')
  parser.add_argument('--site', type=int, help='Site id (integer)')
  parser.add_argument('--key', type=str, help='Access key (string of 32 digits and uppser case letters)')
  return parser.parse_args()

def main():
  args = parseCommandLine()
  if not args.site or not args.key:
    sys.exit('Error: both --site and --key are required')
  start_date, end_date = getSiteDates(args.site, args.key)
  end_date += datetime.timedelta(days=1)
  getSiteData(args.site, args.key, start_date, end_date)

main()
