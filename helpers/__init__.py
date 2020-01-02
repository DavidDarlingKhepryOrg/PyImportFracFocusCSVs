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

import hashlib
import os
import yaml

resources_default_folder = "resources"
resources_tweaked_folder = "resources"
app_settings_file_name = "settings/app_settings.yaml"


def get_settings(settings_file_name,
                 resources_default_fldr=resources_default_folder,
                 resources_tweaked_fldr=resources_tweaked_folder):
    resources_path = os.environ.get('RESOURCES_DEFAULT_PATH', resources_default_fldr)
    # resources_path = os.path.abspath(resources_path)
    if os.path.exists(resources_path):
        msg = f"resources_path (default): {os.path.abspath(resources_path)}"
        print(msg)
    else:
        resources_path = os.environ.get('RESOURCES_TWEAKED_PATH', resources_tweaked_fldr)
        # resources_path = os.path.abspath(resources_path)
        msg = f"resources_path (tweaked): {os.path.abspath(resources_path)}"
        print(msg)
    with open(f"{resources_path}/{settings_file_name}") as yml_file:
        settings = yaml.full_load(yml_file)
    return settings


def get_tgt_src_col_info(settings):
    tgt_col_names = []
    tgt_src_col_dict = {}
    for key, value in settings['tgt_src_col_xlations'].items():
        tgt_col_names.append(key)
        tgt_src_col_dict[key] = value['src_col_name']
    return tgt_src_col_dict, tgt_col_names


def get_bool_xlations(settings):
    bool_xlations = {}
    for key, value in settings['tgt_src_col_xlations'].items():
        if value['data_type'] in ['bool']:
            bool_xlations[key] = value['bool_dict']
    return bool_xlations


def get_date_xlations(settings):
    date_xlations = {}
    for key, value in settings['tgt_src_col_xlations'].items():
        if value['data_type'] in ['datetime', 'date']:
            src_date_fmt = value['date_fmt_src']
            tgt_date_fmt = value['date_fmt_tgt']
            date_xlations[key] = (src_date_fmt, tgt_date_fmt)
    return date_xlations


def get_hex_digest_for_row(row):
    text_str = ''
    for key, value in row.items():
        text_str += f'{key}:{value};'
    return hashlib.sha256(text_str.encode()).hexdigest()
