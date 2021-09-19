#!/usr/bin/env python3

import argparse
import csv
import datetime
import requests
import sys
import time

url_base='https://monitoringapi.solaredge.com'

# Mapping of the API results headers to GUI results headers, by order from left
# to right:
# API -> GUI
# 0: Consumption -> Consumption
# 1: FeedIn -> Export
# 2: Purchage -> Import
# 3: SelfConsumption -> Self Consumption
# 4: Production -> System Production
# The order of the API headers changes in every call and has to be re-mapped

ColumnHeaderMap = {
  'Consumption': 0,
  'FeedIn': 1,
  'Purchased': 2,
  'SelfConsumption': 3,
  'Production': 4,
}

# Generates an ordered list. The place of the list member is the order
# we want (Consumption, Export, Import, Self Consumption, System Production)
# the value of the members is the index of that column as was returned by
# the API in powerDetails
def mapColumns(powerDetails):
  columnMap = [None] * len(ColumnHeaderMap)
  for place, column in enumerate(powerDetails):
    type = column['type']
    columnMap[ColumnHeaderMap[type]] = place
  return columnMap

def _string_to_datetime(date):
  return datetime.datetime.fromtimestamp(time.mktime(time.strptime(date, '%Y-%m-%d')))

def getSiteDates(site, key):
  resp = requests.get(url=f'{url_base}/site/{site}/dataPeriod',
    params=dict(
      api_key=key,
    ))
  if resp.status_code != 200:
    sys.exit(f'Failed to get site date: {resp.text}')

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
      meters='CONSUMPTION,FEEDIN,PURCHASED,SELFCONSUMPTION,PRODUCTION',
    ))
  return resp.json()

def monthly_it(start_date, end_date):
  yield start_date
  while start_date < end_date:
    start_date += datetime.timedelta(weeks=4)
    yield start_date

def getSiteData(site, key, start_date, end_date, outfile):
  csvwriter = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)
  for month in monthly_it(start_date, end_date):
    next_month = month + datetime.timedelta(weeks=4)
    if next_month > end_date:
      next_month = end_date
    powerDetails = getSitePowerDetails(site, key, month, next_month)['powerDetails']['meters']
    columnMap = mapColumns(powerDetails)
    # this is the first row - print the headers
    if month == start_date:
      headers=['Time']
      for column in columnMap:
        headers.append(powerDetails[column]['type'])
      csvwriter.writerow(headers)

    for index in range (len(powerDetails[0]['values'])):
      row=[]
      timeStamp = time.strftime('%d/%m/%Y %H:%M', time.strptime(powerDetails[0]['values'][index]['date'], '%Y-%m-%d %H:%M:%S'))
      row.append(timeStamp)
      for column in columnMap:
        row.append(str(round(powerDetails[column]['values'][index].get('value', 0), 4)).rstrip('.0') or '0')
      csvwriter.writerow(row)

def parseCommandLine():
  parser = argparse.ArgumentParser(description='Pull SolarEdge data into CSV')
  parser.add_argument('--site', type=int, help='Site id (integer)')
  parser.add_argument('--key', type=str, help='Access key (string of 32 digits and uppser case letters)')
  parser.add_argument('--out', type=str, help='Name of output CSV file')
  return parser.parse_args()

def main():
  args = parseCommandLine()
  if not args.site or not args.key:
    sys.exit('Error: both --site and --key are required')
  if args.out:
    outfile = open(args.out, 'w', newline='')
  else:
    outfile = sys.stdout
  start_date, end_date = getSiteDates(args.site, args.key)
  end_date += datetime.timedelta(days=1)
  getSiteData(args.site, args.key, start_date, end_date, outfile)

main()
