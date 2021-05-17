from metaflow import (
    FlowSpec,
    step,
    Parameter)

import pandas as pd
import requests


def metric_per_file(json):

    file_json = []

    for component in json['components']:
        if component['qualifier'] == 'FIL':
            file_json.append(component)

    return file_json


def generate_file_dataframe(metric_list, json, language_extension):
    df_columns = metric_list
    df = pd.DataFrame(columns=df_columns)

    for file in json:
        try:
            if file['language'] == language_extension:
                for measure in file['measures']:
                    df.at[file['path'], measure['metric']] = measure['value']
        except:
            pass

    df.reset_index(inplace=True)
    df = df.rename({'index': 'path'}, axis=1).drop(['files'], axis=1)

    return df


def m1(df):

    density_non_complex_files = len(
        df[(df['complexity'].astype(float)/df['functions'].astype(float)) < 10])/len(df)

    return density_non_complex_files


def m2(df):

    density_comment_files = len(df[(df['comment_lines_density'].astype(
        float) > 10) & (df['comment_lines_density'].astype(float) < 30)])/len(df)

    return density_comment_files


def m3(df):

    duplication = len(
        df[(df['duplicated_lines_density'].astype(float) < 5)])/len(df)

    return duplication


def m7(number_of_issues_resolved, number_of_issues):

    resolved_issues_throughput = round(
        (number_of_issues_resolved / number_of_issues) * 100, 2)

    return resolved_issues_throughput


def density(issue, number_of_issues):
    issue_density = round((issue / number_of_issues) * 100, 2)
    return issue_density


def m8(tag_dict, number_of_issues):

    issue_densities = {
        "hotfix": [density(tag_dict["HOTFIX"], number_of_issues)],
        "docs": [density(tag_dict["DOCS"], number_of_issues)],
        "feature": [density(tag_dict["FEATURE"], number_of_issues)],
        "arq": [density(tag_dict["ARQ"], number_of_issues)],
        "devops": [density(tag_dict["DEVOPS"], number_of_issues)],
        "analytics": [density(tag_dict["ANALYTICS"], number_of_issues)],
        "us": [density(tag_dict["US"], number_of_issues)],
        "easy": [density(tag_dict["EASY"], number_of_issues)],
        "medium": [density(tag_dict["MEDIUM"], number_of_issues)],
        "hard": [density(tag_dict["HARD"], number_of_issues)],
        "eps": [density(tag_dict["EPS"], number_of_issues)],
        "mds": [density(tag_dict["MDS"], number_of_issues)]
    }

    issue_densities = pd.DataFrame.from_dict(issue_densities).T.reset_index()

    issue_densities.columns = ['density', 'percentage']

    return issue_densities


def m9(tag_dict, number_of_issues):

    bugs_ratio = round(((tag_dict["DOCS"] + tag_dict["FEATURE"] + tag_dict["ARQ"] +
                         tag_dict["DEVOPS"] + tag_dict["ANALYTICS"]) / number_of_issues) * 100, 2)

    return bugs_ratio


def asc1(m1, m2, m3):
    psc1 = 1
    pm1 = 0.33
    pm2 = 0.33
    pm3 = 0.33

    asc1_result = ((m1 * pm1) + (m2 * pm2) + (m3 * pm3)) * psc1
    return asc1_result


class DataProcessing(FlowSpec):
    metrics_list = Parameter(
        "metrics_list", help="Metrics to use",
        default=['files',
                 'functions',
                 'complexity',
                 'comment_lines_density',
                 'duplicated_lines_density',
                 'coverage',
                 'ncloc',
                 'security_rating',
                 'tests',
                 'test_success_density',
                 'test_execution_time',
                 'reliability_rating'])

    services_language_extension = Parameter(
        'services_language', help="Services languages extension",
        default={
            'user': 'js',
            'gateway': 'js',
            'request': 'py',
            'rating': 'py'
        }
    )

    @step
    def start(self):
        from collections import defaultdict
        import re

        services_tmp = defaultdict(lambda: defaultdict(dict))

        releases_folder = requests.get(
            'https://api.github.com/repos/fga-eps-mds/2020.2-Lend.it/contents/analytics-raw-data').json()

        for release in releases_folder:
            folders_json = requests.get(release['url']).json()

            for json_file in folders_json:
                service_name = re.search(
                    r'(\w+)-\d{2}-\d{2}-\d{4}\.json', json_file['name'])[1]

                services_tmp[service_name][release['name'].lower()]['json'] = requests.get(
                    json_file['download_url']).json()

        for service_name, releases in services_tmp.items():
            tmp_dict = {}

            for release_name, content in releases.items():
                tmp_dict[release_name] = dict(
                    content)

            services_tmp[service_name] = tmp_dict

        self.services_metrics = dict(services_tmp)

        self.next(self.create_dataframe)

    @step
    def create_dataframe(self):

        for service_name, releases in self.services_metrics.items():
            for release_name, content in releases.items():
                df = pd.DataFrame(content['json']['baseComponent']['measures'])

                self.services_metrics[service_name][release_name]['df'] = df

        self.next(self.create_file_metrics_df)

    @step
    def create_file_metrics_df(self):

        for service_name, releases in self.services_metrics.items():
            for release_name, content in releases.items():
                file_component_data = [
                    component for component in content['json']['components'] if component['qualifier'] == 'FIL']
                self.services_metrics[service_name][release_name]['files_df'] = generate_file_dataframe(
                    self.metrics_list, file_component_data, language_extension=self.services_language_extension[service_name])

        self.next(self.create_service_metrics_df)

    @step
    def create_service_metrics_df(self):
        self.metrics_dfs = {}

        for service_name, releases in self.services_metrics.items():
            service_df = pd.DataFrame(columns=['m1',
                                               'm2',
                                               'm3',
                                               'm7',
                                               'm8',
                                               'm9',
                                               'asc1',
                                               'ac1',
                                               'asc2',
                                               'ac2',
                                               'totalAC1',
                                               'totalAC2',
                                               'ncloc'])

            for release_name, content in releases.items():
                metrics = {}
                base_component_df = self.services_metrics[service_name][release_name]['df']

                metrics['m1'] = m1(content['files_df'])
                metrics['m2'] = m2(content['files_df'])
                metrics['m3'] = m3(content['files_df'])
                metrics['asc1'] = asc1(
                    metrics['m1'], metrics['m2'], metrics['m3'])
                metrics['ac1'] = asc1(
                    metrics['m1'], metrics['m2'], metrics['m3'])
                metrics['totalAC1'] = asc1(
                    metrics['m1'], metrics['m2'], metrics['m3'])
                metrics['ncloc'] = int(base_component_df[base_component_df['metric']
                                                         == 'ncloc']['value'].values[0])

                service_df.loc[release_name] = metrics

            self.metrics_dfs[service_name] = service_df

        self.next(self.end)

    @ step
    def end(self):
        pass


if __name__ == "__main__":
    DataProcessing()
