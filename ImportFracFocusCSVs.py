"""
MIT License

Copyright (c) 2019 Khepry Quixote

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""

import csv
import os
# import sys

from datetime import datetime
from pprint import pprint

from helpers import app_settings_file_name
from helpers import get_settings, get_tgt_src_col_info, get_bool_xlations, get_date_xlations, get_hex_digest_for_row
from helpers import resources_default_folder, resources_tweaked_folder

print('')

APP_SETTINGS = get_settings(app_settings_file_name,
                            resources_default_fldr=resources_default_folder,
                            resources_tweaked_fldr=resources_tweaked_folder)

process_only_first_file = APP_SETTINGS['process_only_first_file']

hex_digest_col_name = APP_SETTINGS["hex_digest_col_name"]

tgt_src_col_info, tgt_col_names = get_tgt_src_col_info(APP_SETTINGS)
date_xlations = get_date_xlations(APP_SETTINGS)
bool_xlations = get_bool_xlations(APP_SETTINGS)

src_csv_folder = APP_SETTINGS["src_csv_folder"]
tgt_csv_folder = APP_SETTINGS['tgt_csv_folder']
toxicities_folder = APP_SETTINGS["toxicities_folder"]

src_file_pattern = APP_SETTINGS["src_file_pattern"]

if src_csv_folder.startswith("~"):
    src_csv_folder = os.path.expanduser(src_csv_folder)
if tgt_csv_folder.startswith("~"):
    tgt_csv_folder = os.path.expanduser(tgt_csv_folder)

os.makedirs(src_csv_folder, exist_ok=True)
os.makedirs(tgt_csv_folder, exist_ok=True)
os.makedirs(toxicities_folder, exist_ok=True)

print('')
print(f'src_csv_folder: {src_csv_folder}')
print(f'tgt_csv_folder: {tgt_csv_folder}')
print(f'toxicities_folder: {toxicities_folder}')
print(f'src_file_pattern: {src_file_pattern}')
print('')

disclosure_keys = {}
disclosure_col_max_sizes = {}
disclosure_col_names = APP_SETTINGS['disclosure_file_info']['col_names']
disclosure_file_name = os.path.join(tgt_csv_folder, APP_SETTINGS['disclosure_file_info']['base_name'] + '.csv')
disclosure_file = open(disclosure_file_name, "w", newline="", encoding="utf-8")
disclosure_writer = csv.DictWriter(disclosure_file,
                                   quoting=csv.QUOTE_MINIMAL,
                                   quotechar='"',
                                   delimiter=',',
                                   fieldnames=disclosure_col_names)
disclosure_writer.writeheader()

purpose_keys = {}
purpose_col_max_sizes = {}
purpose_col_names = APP_SETTINGS['purpose_file_info']['col_names']
purpose_file_name = os.path.join(tgt_csv_folder, APP_SETTINGS['purpose_file_info']['base_name'] + '.csv')
purpose_file = open(purpose_file_name, "w", newline="", encoding="utf-8")
purpose_writer = csv.DictWriter(purpose_file,
                                quoting=csv.QUOTE_MINIMAL,
                                quotechar='"',
                                delimiter=',',
                                fieldnames=purpose_col_names)
purpose_writer.writeheader()

ingredient_keys = {}
ingredient_col_max_sizes = {}
ingredient_col_names = APP_SETTINGS['ingredient_file_info']['col_names']
ingredient_file_name = os.path.join(tgt_csv_folder, APP_SETTINGS['ingredient_file_info']['base_name'] + '.csv')
ingredient_file = open(ingredient_file_name, "w", newline="", encoding="utf-8")
ingredient_writer = csv.DictWriter(ingredient_file,
                                   quoting=csv.QUOTE_MINIMAL,
                                   quotechar='"',
                                   delimiter=',',
                                   fieldnames=ingredient_col_names)
ingredient_writer.writeheader()

# rename the FracFocus CSV files so that
# when "walked" later, they'll be imported
# in the same order as they were numbered
for root, dirs, files in os.walk(src_csv_folder):
    for file in files:
        if file.startswith(src_file_pattern):
            underscore_ndx = file.find("_")
            extension_text = os.path.splitext(file)[1]
            extension_ndx = file.find(extension_text)
            file_nbr = file[underscore_ndx+1: extension_ndx]
            file_nbr = ('0' + file_nbr)[-2:]
            file_tgt_name = file[:underscore_ndx+1] + file_nbr + extension_text
            file_src_name = os.path.join(root, file)
            file_tgt_name = os.path.join(root, file_tgt_name)
            if not os.path.exists(file_tgt_name):
                print(f'Rename FracFocus CSV file: {file_src_name} to: {file_tgt_name}')
                os.rename(file_src_name, file_tgt_name)
    break

# sys.exit(0)

# translate the FracFocus CSV files as
# specified in the app_settings YAML file
for root, dirs, files in os.walk(src_csv_folder):
    for file in files:
        if file.startswith(src_file_pattern):
            src_file_name = os.path.join(root, file)
            tgt_file_name = os.path.join(tgt_csv_folder, file)
            print(f'tgt_csv_file_name: {tgt_file_name}')
            with open(tgt_file_name, "w", newline="", encoding="utf-8") as tgt_file:

                csv_writer = csv.DictWriter(tgt_file,
                                            quoting=csv.QUOTE_MINIMAL,
                                            quotechar='"',
                                            delimiter=',',
                                            fieldnames=tgt_col_names)
                csv_writer.writeheader()

                with open(src_file_name, "r", newline="", encoding="utf-8") as src_file:
                    csv_reader = csv.DictReader(src_file,
                                                quoting=csv.QUOTE_MINIMAL,
                                                quotechar='"',
                                                delimiter=',')
                    tgt_row = {}
                    for row in csv_reader:

                        # rename any source columns to target column names
                        for tgt_col_name, src_col_name in tgt_src_col_info.items():
                            if src_col_name is not None:
                                tgt_row[tgt_col_name] = row[src_col_name]
                            else:
                                tgt_row[tgt_col_name] = None

                        # reformat any date columns
                        for key, value in date_xlations.items():
                            if key in tgt_row and tgt_row[key] != '':
                                tgt_row[key] = datetime.strptime(tgt_row[key], value[0]).strftime(value[1])

                        # reformat any boolean columns
                        for key, value in bool_xlations.items():
                            if key in tgt_row and tgt_row[key] != '':
                                tgt_row[key] = value[tgt_row[key]]

                        # compute the hex digest for the target row
                        tgt_row[hex_digest_col_name] = get_hex_digest_for_row(tgt_row)

                        # write the target row
                        # to the target CSV file
                        csv_writer.writerow(tgt_row)

                        # write appropriate columns to the disclosures CSV file
                        disclosure_row = {key: value for key, value in tgt_row.items() if key in disclosure_col_names}
                        disclosure_key = disclosure_row['disclosure_key']
                        if disclosure_key not in disclosure_keys:
                            disclosure_row[hex_digest_col_name] = get_hex_digest_for_row(disclosure_row)
                            disclosure_keys[disclosure_key] = None
                            disclosure_writer.writerow(disclosure_row)
                            # discern the max column size
                            # for each of the target columns
                            for key, value in disclosure_row.items():
                                try:
                                    if type(value) is str and len(value) > disclosure_col_max_sizes[key]:
                                        disclosure_col_max_sizes[key] = len(value)
                                except KeyError:
                                    disclosure_col_max_sizes[key] = len(value)

                        # write appropriate columns to the purposes CSV file
                        purpose_row = {key: value for key, value in tgt_row.items() if key in purpose_col_names}
                        purpose_key = purpose_row['purpose_key']
                        if purpose_key not in purpose_keys:
                            purpose_row[hex_digest_col_name] = get_hex_digest_for_row(purpose_row)
                            purpose_keys[purpose_key] = None
                            purpose_writer.writerow(purpose_row)
                            # discern the max column size
                            # for each of the target columns
                            for key, value in purpose_row.items():
                                try:
                                    if type(value) is str and len(value) > purpose_col_max_sizes[key]:
                                        purpose_col_max_sizes[key] = len(value)
                                except KeyError:
                                    purpose_col_max_sizes[key] = len(value)

                        # write appropriate columns to the ingredients CSV file
                        ingredient_row = {key: value for key, value in tgt_row.items() if key in ingredient_col_names}
                        ingredient_key = ingredient_row['ingredient_key']
                        if ingredient_key not in ingredient_keys:
                            ingredient_row[hex_digest_col_name] = get_hex_digest_for_row(ingredient_row)
                            ingredient_keys[ingredient_key] = None
                            ingredient_writer.writerow(ingredient_row)
                            # discern the max column size
                            # for each of the target columns
                            for key, value in ingredient_row.items():
                                try:
                                    if type(value) is str and len(value) > ingredient_col_max_sizes[key]:
                                        ingredient_col_max_sizes[key] = len(value)
                                except KeyError:
                                    ingredient_col_max_sizes[key] = len(value)

            # flush rows to files
            disclosure_file.flush()
            purpose_file.flush()
            ingredient_file.flush()

            if process_only_first_file:
                break

    break

# close target files
ingredient_file.close()
purpose_file.close()
disclosure_file.close()

print('')

print('disclosures columns maximum sizes')
pprint(disclosure_col_max_sizes)

print('purpose columns maximum sizes')
pprint(purpose_col_max_sizes)

print('ingredients columns maximum sizes')
pprint(ingredient_col_max_sizes)
